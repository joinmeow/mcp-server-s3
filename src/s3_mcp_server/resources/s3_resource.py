import os
from datetime import datetime
from typing import TypedDict
import aioboto3
from botocore.config import Config
from types_aiobotocore_s3.type_defs import BucketTypeDef, ObjectTypeDef


class ObjectMetadata(TypedDict):
    content_type: str
    size_bytes: int
    last_modified: str | None


class S3ObjectData(TypedDict):
    """The subset of S3 GetObject response fields we use, after reading the stream into bytes."""
    Body: bytes
    ContentType: str
    ContentLength: int
    LastModified: datetime | None


class SaveResult(TypedDict):
    saved_to: str
    size_bytes: int
    content_type: str
    last_modified: str | None


class BatchFileResult(TypedDict):
    key: str
    saved_to: str
    size_bytes: int
    content_type: str
    last_modified: str | None


class BatchError(TypedDict):
    key: str
    error: str


class BatchResult(TypedDict):
    files_saved: int
    files: list[BatchFileResult]
    errors: list[BatchError]


class S3Resource:
    """S3 Resource provider that handles interactions with AWS S3 buckets."""

    def __init__(self, region_name: str | None = None, profile_name: str | None = None, max_buckets: int = 5):
        self.config = Config(
            retries=dict(max_attempts=3, mode='adaptive'),
            connect_timeout=5,
            read_timeout=60,
        )
        self.session = aioboto3.Session(
            profile_name=profile_name,
            region_name=region_name,
        )
        self.region_name = region_name
        self.max_buckets = max_buckets
        self.configured_buckets = self._get_configured_buckets()

    def _get_configured_buckets(self) -> list[str]:
        """Read allowed bucket names from S3_BUCKETS or S3_BUCKET_N env vars."""
        bucket_list = os.getenv('S3_BUCKETS')
        if bucket_list:
            return [b.strip() for b in bucket_list.split(',')]

        buckets: list[str] = []
        i = 1
        while True:
            bucket = os.getenv(f'S3_BUCKET_{i}')
            if not bucket:
                break
            buckets.append(bucket.strip())
            i += 1
        return buckets

    def _check_bucket(self, bucket_name: str) -> None:
        """Raise if bucket_name is not in the configured allowlist."""
        if self.configured_buckets and bucket_name not in self.configured_buckets:
            raise ValueError(f"Bucket {bucket_name} not in configured bucket list")

    async def list_buckets(self, start_after: str | None = None) -> list[BucketTypeDef]:
        async with self.session.client('s3', region_name=self.region_name) as s3:
            response = await s3.list_buckets()
            buckets = response.get('Buckets', [])

            if self.configured_buckets:
                buckets = [b for b in buckets if b['Name'] in self.configured_buckets]

            if start_after:
                buckets = [b for b in buckets if b['Name'] > start_after]

            return buckets[:self.max_buckets]

    async def list_objects(self, bucket_name: str, prefix: str = "", max_keys: int = 1000) -> list[ObjectTypeDef]:
        self._check_bucket(bucket_name)
        async with self.session.client('s3', region_name=self.region_name) as s3:
            response = await s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys,
            )
            return response.get('Contents', [])

    async def head_object(self, bucket_name: str, key: str) -> ObjectMetadata:
        self._check_bucket(bucket_name)
        async with self.session.client('s3', region_name=self.region_name, config=self.config) as s3:
            response = await s3.head_object(Bucket=bucket_name, Key=key)
            last_modified = response.get("LastModified")
            return {
                "content_type": response.get("ContentType", "application/octet-stream"),
                "size_bytes": response.get("ContentLength", 0),
                "last_modified": last_modified.isoformat() if last_modified else None,
            }

    async def get_object(self, bucket_name: str, key: str) -> S3ObjectData:
        """Download an S3 object, reading the full stream into bytes."""
        self._check_bucket(bucket_name)
        async with self.session.client('s3', region_name=self.region_name, config=self.config) as s3:
            response = await s3.get_object(Bucket=bucket_name, Key=key)

            chunks: list[bytes] = []
            async for chunk in response['Body']:
                chunks.append(chunk)

            response['Body'] = b''.join(chunks)
            return response  # type: ignore[return-value]

    async def save_object_to_file(self, bucket_name: str, key: str, output_path: str) -> SaveResult:
        """Download an S3 object and save it to a local file."""
        response = await self.get_object(bucket_name, key)
        data = response['Body']

        if os.path.isdir(output_path) or output_path.endswith('/'):
            output_path = os.path.join(output_path, os.path.basename(key))

        parent_dir = os.path.dirname(output_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        with open(output_path, 'wb') as f:
            f.write(data)

        last_modified = response.get("LastModified")
        return {
            "saved_to": output_path,
            "size_bytes": len(data),
            "content_type": response.get("ContentType", "application/octet-stream"),
            "last_modified": last_modified.isoformat() if last_modified else None,
        }

    async def get_objects_batch(
        self,
        bucket_name: str,
        output_dir: str,
        keys: list[str] | None = None,
        prefix: str | None = None,
        max_bytes: int | None = None,
    ) -> BatchResult:
        """Download multiple S3 objects to a local directory."""
        if not keys and prefix is None:
            raise ValueError("Either 'keys' or 'prefix' must be provided")

        if not keys:
            objects = await self.list_objects(bucket_name, prefix=prefix or "")
            keys = [obj['Key'] for obj in objects]

        os.makedirs(output_dir, exist_ok=True)
        results: BatchResult = {"files_saved": 0, "files": [], "errors": []}

        # Strip shared prefix so subdirectory structure is preserved under output_dir
        # e.g. prefix="reports/" keys=["reports/a/1.pdf","reports/b/2.pdf"]
        #   → output_dir/a/1.pdf, output_dir/b/2.pdf
        common_prefix = os.path.commonprefix(keys) if keys else ""
        # Trim to last '/' so we don't chop a partial directory name
        common_prefix = common_prefix[:common_prefix.rfind('/') + 1]

        for key in keys:
            try:
                if max_bytes:
                    meta = await self.head_object(bucket_name, key)
                    if meta["size_bytes"] > max_bytes:
                        results["errors"].append({
                            "key": key,
                            "error": f"Object size ({meta['size_bytes']} bytes) exceeds max_bytes limit ({max_bytes})",
                        })
                        continue

                relative = key[len(common_prefix):] if common_prefix else os.path.basename(key)
                output_path = os.path.join(output_dir, relative)
                result = await self.save_object_to_file(bucket_name, key, output_path)
                results["files"].append({"key": key, **result})
                results["files_saved"] += 1
            except Exception as e:
                results["errors"].append({"key": key, "error": str(e)})

        return results

    def extract_text_from_pdf(self, data: bytes) -> str:
        """Extract text content from PDF bytes. Requires pymupdf."""
        try:
            import fitz  # pymupdf
        except ImportError:
            raise ImportError(
                "pymupdf is required for PDF text extraction. "
                "Install with: pip install 's3-mcp-server[pdf]' or pip install pymupdf"
            )
        doc = fitz.open(stream=data, filetype="pdf")
        pages = [page.get_text() for page in doc]
        doc.close()
        return "\n".join(pages)

    def is_text_file(self, key: str, content_type: str = "") -> bool:
        """Determine if a file is text-based by its extension or content type."""
        text_extensions = {
            '.txt', '.log', '.json', '.xml', '.yml', '.yaml', '.md',
            '.csv', '.ini', '.conf', '.py', '.js', '.html', '.css',
            '.sh', '.bash', '.cfg', '.properties', '.ts', '.tsx',
            '.jsx', '.sql', '.env', '.toml', '.rst', '.tex',
        }
        if key.lower().endswith(tuple(text_extensions)):
            return True

        if content_type:
            if content_type.startswith('text/'):
                return True
            if content_type in {
                'application/json', 'application/xml',
                'application/javascript', 'application/x-yaml',
                'application/toml', 'application/sql',
                'application/x-sh',
            }:
                return True

        return False
