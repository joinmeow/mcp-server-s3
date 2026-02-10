import asyncio
import base64
import json
import os

from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
from dotenv import load_dotenv
import logging
from mcp.types import (
    LoggingLevel, EmptyResult, Tool,
    TextContent, EmbeddedResource, BlobResourceContents,
)

from .resources.s3_resource import S3Resource

server = Server("s3_service")

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp_s3_server")

max_buckets = int(os.getenv('S3_MAX_BUCKETS', '5'))
aws_profile = os.getenv('AWS_PROFILE')
aws_region = os.getenv('AWS_REGION', 'us-east-1')

s3_resource = S3Resource(
    region_name=aws_region,
    profile_name=aws_profile,
    max_buckets=max_buckets,
)


@server.set_logging_level()
async def set_logging_level(level: LoggingLevel) -> EmptyResult:
    logger.setLevel(level.lower())
    await server.request_context.session.send_log_message(
        level="info",
        data=f"Log level set to {level}",
        logger="mcp_s3_server",
    )
    return EmptyResult()


@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    return [
        Tool(
            name="ListBuckets",
            description=(
                "List available S3 buckets. Returns bucket names and creation dates as JSON. "
                "Call this first to discover which buckets you can access."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "start_after": {
                        "type": "string",
                        "description": "Only return buckets whose name is alphabetically after this value. Used for pagination.",
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="ListObjectsV2",
            description=(
                "List objects (files) in an S3 bucket. Returns key, size, and last-modified for each object as JSON. "
                "Use prefix to filter to a subdirectory. Returns up to max_keys results (default 1000)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {
                        "type": "string",
                        "description": "Name of the S3 bucket.",
                    },
                    "prefix": {
                        "type": "string",
                        "description": "Only return keys that start with this prefix. Use like a directory path (e.g. 'reports/2024/').",
                    },
                    "max_keys": {
                        "type": "integer",
                        "description": "Maximum number of objects to return (default: 1000, max: 1000).",
                    },
                },
                "required": ["bucket_name"],
            },
        ),
        Tool(
            name="GetObject",
            description=(
                "Download a single file from S3. "
                "Text files (.json, .csv, .txt, .xml, etc.) are returned as plain text. "
                "For PDFs you need to read, set extract_text=true to get readable text from all pages. "
                "For binary files (PDFs, images, archives), always use output_path to save to disk — "
                "e.g. output_path='/tmp/report.pdf'. Without output_path, binary files are returned "
                "as base64 which is not useful for further processing. "
                "Use max_bytes to check file size before downloading. "
                "Every response includes metadata: content_type, size_bytes, last_modified."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {
                        "type": "string",
                        "description": "Name of the S3 bucket.",
                    },
                    "key": {
                        "type": "string",
                        "description": "Full path of the object in the bucket (e.g. 'reports/2024/q1.pdf').",
                    },
                    "output_path": {
                        "type": "string",
                        "description": (
                            "Save the file to this local path instead of returning content inline. "
                            "Recommended for binary files (PDFs, images, archives) — "
                            "e.g. '/tmp/downloads/report.pdf' or '/tmp/downloads/' (filename from key). "
                            "Parent directories are created automatically. "
                            "Response will be JSON with saved_to, size_bytes, content_type, last_modified."
                        ),
                    },
                    "max_bytes": {
                        "type": "integer",
                        "description": (
                            "Maximum allowed file size in bytes. If the file exceeds this, "
                            "an error is returned immediately (uses HEAD request, no download). "
                            "Example: 500000 for ~500KB limit."
                        ),
                    },
                    "extract_text": {
                        "type": "boolean",
                        "description": (
                            "Set to true when retrieving PDFs to get readable text instead of base64-encoded binary. "
                            "The text is extracted from all pages and returned as plain text alongside metadata. "
                            "Recommended for any PDF you need to read or analyze."
                        ),
                    },
                    "max_retries": {
                        "type": "integer",
                        "description": "Number of download attempts on transient failures (default: 3).",
                    },
                },
                "required": ["bucket_name", "key"],
            },
        ),
        Tool(
            name="GetObjects",
            description=(
                "Batch download multiple files from S3 to a local directory in one call. "
                "Provide either a list of keys OR a prefix to download all matching files. "
                "Returns JSON summary: files_saved count, file paths with metadata, and any errors. "
                "Example: GetObjects(bucket_name='my-bucket', prefix='case-35117/', output_dir='/tmp/35117/') "
                "downloads all files under that prefix — replaces ListObjectsV2 + N separate GetObject calls."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {
                        "type": "string",
                        "description": "Name of the S3 bucket.",
                    },
                    "keys": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Explicit list of object keys to download. Takes precedence over prefix if both given.",
                    },
                    "prefix": {
                        "type": "string",
                        "description": "Download all objects whose key starts with this prefix.",
                    },
                    "output_dir": {
                        "type": "string",
                        "description": "Local directory to save files into. Created automatically if it doesn't exist.",
                    },
                    "max_bytes": {
                        "type": "integer",
                        "description": "Skip any file larger than this many bytes.",
                    },
                    "max_retries": {
                        "type": "integer",
                        "description": "Number of download attempts per file on transient failures (default: 3).",
                    },
                },
                "required": ["bucket_name", "output_dir"],
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None,
) -> list[TextContent | EmbeddedResource]:
    try:
        match name:
            case "ListBuckets":
                start_after = arguments.get("start_after") if arguments else None
                buckets = await s3_resource.list_buckets(start_after)
                return [TextContent(type="text", text=json.dumps(buckets, default=str))]

            case "ListObjectsV2":
                bucket_name = arguments['bucket_name']
                prefix = arguments.get('prefix', "")
                max_keys = arguments.get('max_keys', 1000)

                objects = await s3_resource.list_objects(bucket_name, prefix, max_keys)
                return [TextContent(type="text", text=json.dumps(objects, default=str))]

            case "GetObject":
                bucket_name = arguments['bucket_name']
                key = arguments['key']
                output_path = arguments.get('output_path')
                max_bytes = arguments.get('max_bytes')
                extract_text = arguments.get('extract_text', False)
                max_retries = int(arguments.get('max_retries', 3))

                # Size guard — HEAD only, no body download
                if max_bytes:
                    meta = await s3_resource.head_object(bucket_name, key)
                    if meta["size_bytes"] > int(max_bytes):
                        return [TextContent(type="text", text=json.dumps({
                            "error": f"Object size ({meta['size_bytes']} bytes) exceeds max_bytes limit ({max_bytes})",
                            **meta,
                            "key": key,
                            "bucket": bucket_name,
                        }))]

                # Save-to-disk mode
                if output_path:
                    result = await s3_resource.save_object_to_file(
                        bucket_name, key, output_path, max_retries=max_retries,
                    )
                    return [TextContent(type="text", text=json.dumps({
                        "key": key,
                        "bucket": bucket_name,
                        **result,
                    }))]

                # Inline mode — download full body
                response = await s3_resource.get_object(bucket_name, key, max_retries=max_retries)
                content_type = response.get("ContentType", "application/octet-stream")
                data = response['Body']
                last_modified = response.get("LastModified")

                metadata = {
                    "key": key,
                    "bucket": bucket_name,
                    "content_type": content_type,
                    "size_bytes": len(data),
                    "last_modified": last_modified.isoformat() if last_modified else None,
                }
                metadata_content = TextContent(type="text", text=json.dumps(metadata))

                # PDF text extraction
                if extract_text and key.lower().endswith('.pdf'):
                    try:
                        text = s3_resource.extract_text_from_pdf(data)
                        return [metadata_content, TextContent(type="text", text=text)]
                    except ImportError as ie:
                        return [TextContent(type="text", text=json.dumps({
                            "error": str(ie), **metadata,
                        }))]

                # Text files → plain text
                if s3_resource.is_text_file(key, content_type):
                    return [metadata_content, TextContent(type="text", text=data.decode('utf-8'))]

                # Binary files → base64 blob
                return [
                    metadata_content,
                    EmbeddedResource(
                        type="resource",
                        resource=BlobResourceContents(
                            blob=base64.b64encode(data).decode('utf-8'),
                            uri=f"s3://{bucket_name}/{key}",
                            mimeType=content_type,
                        ),
                    ),
                ]

            case "GetObjects":
                result = await s3_resource.get_objects_batch(
                    bucket_name=arguments['bucket_name'],
                    output_dir=arguments['output_dir'],
                    keys=arguments.get('keys'),
                    prefix=arguments.get('prefix'),
                    max_bytes=arguments.get('max_bytes'),
                    max_retries=int(arguments.get('max_retries', 3)),
                )
                return [TextContent(type="text", text=json.dumps(result, default=str))]

            case _:
                return [TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))]

    except Exception as error:
        return [TextContent(type="text", text=json.dumps({"error": str(error)}))]


async def main():
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="s3-mcp-server",
                server_version="0.2.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
