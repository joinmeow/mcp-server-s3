import asyncio

from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
from mcp import McpError
import mcp.server.stdio
from dotenv import load_dotenv
import logging
import os
from mcp.types import LoggingLevel, EmptyResult, Tool, TextContent, ImageContent, EmbeddedResource, BlobResourceContents

from .resources.s3_resource import S3Resource

import base64

# Initialize server
server = Server("s3_service")

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp_s3_server")

# Get configuration from environment
max_buckets = int(os.getenv('S3_MAX_BUCKETS', '5'))
aws_profile = os.getenv('AWS_PROFILE')
aws_region = os.getenv('AWS_REGION', 'us-east-1')

# Initialize S3 resource with profile support (e.g. for AWS SSO)
s3_resource = S3Resource(
    region_name=aws_region,
    profile_name=aws_profile,
    max_buckets=max_buckets
)

@server.set_logging_level()
async def set_logging_level(level: LoggingLevel) -> EmptyResult:
    logger.setLevel(level.lower())
    await server.request_context.session.send_log_message(
        level="info",
        data=f"Log level set to {level}",
        logger="mcp_s3_server"
    )
    return EmptyResult()

@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    return [
        Tool(
            name="ListBuckets", # https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
            description="Returns a list of all buckets owned by the authenticated sender of the request. To grant IAM permission to use this operation, you must add the s3:ListAllMyBuckets policy action.",
            inputSchema={
                "type": "object",
                "properties": {
                    "start_after": {"type": "string", "description": "Start listing after this bucket name"}
                },
                "required": [],
            },
        ),
        Tool(
            name="ListObjectsV2", # https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
            description="Returns some or all (up to 1,000) of the objects in a bucket with each request. You can use the request parameters as selection criteria to return a subset of the objects in a bucket. To get a list of your buckets, see ListBuckets.",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format Bucket_name.s3express-az_id.region.amazonaws.com. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format bucket_base_name--az-id--x-s3 (for example, DOC-EXAMPLE-BUCKET--usw2-az1--x-s3)."},
                    "prefix": {"type": "string", "description": "the prefix of the keys to list."},
                    "max_keys": {"type": "integer", "description": "Sets the maximum number of keys returned in the response. By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more."}
                },
                "required": ["bucket_name"],
            },
        ),
        Tool(
            name="GetObject", # https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
            description="Retrieves an object from Amazon S3. Supports both text files (returned as plain text) and binary files such as PDFs, images, and Office documents (returned as base64-encoded blobs with the appropriate MIME type). Specify the full key name for the object.",
            inputSchema={
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "description": "Directory buckets - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format Bucket_name.s3express-az_id.region.amazonaws.com. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format bucket_base_name--az-id--x-s3 (for example, DOC-EXAMPLE-BUCKET--usw2-az1--x-s3)."},
                    "key": {"type": "string", "description": "Key of the object to get. Length Constraints: Minimum length of 1."},
                    "max_retries": {"type": "string", "description": "max number of attempts to download the file."},
                },
                "required": ["bucket_name", "key"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[TextContent | ImageContent | EmbeddedResource]:
    logger.info(f"handle_call_tool got name {name}, args {arguments}")
    try:
        match name:
            case "ListBuckets":
                start_after = arguments.get("StartAfter", None) if arguments else None
                buckets = await s3_resource.list_buckets(start_after)
                logger.info(f"listBuckets returning buckets {buckets}")
                return [
                    TextContent(
                        type="text",
                        text=str(buckets)
                    )
                ]
            case "ListObjectsV2":
                args = {
                    "bucket_name": arguments['bucket_name']
                }
                if 'prefix' in arguments:
                    args['prefix'] = arguments['prefix']
                if "max_retries" in arguments:
                    args['max_retries'] = arguments['max_retries']

                objects = await s3_resource.list_objects(**args)

                logger.info(f"ListObjectsV2 returning objects {objects}")

                return [
                    TextContent(
                        type="text",
                        text=str(objects)
                    )
                ]
            case "GetObject":
                args = {
                    "bucket_name": arguments['bucket_name'],
                    "key": arguments['key']
                }
                if "max_retries" in arguments:
                    args['max_retries'] = arguments['max_retries']
                response = await s3_resource.get_object(**args)
                content_type = response.get("ContentType", "")
                body = response['Body']
                data = body if isinstance(body, bytes) else await body.read()
                key = arguments['key']

                if s3_resource.is_text_file(key):
                    file_content = data.decode('utf-8')
                    logger.info(f"GetObject returning text for {key}")
                    return [
                        TextContent(
                            type="text",
                            text=file_content
                        )
                    ]
                else:
                    encoded = base64.b64encode(data).decode('utf-8')
                    logger.info(f"GetObject returning base64 blob for {key} ({content_type})")
                    return [
                        EmbeddedResource(
                            type="resource",
                            resource=BlobResourceContents(
                                blob=encoded,
                                uri=f"s3://{arguments['bucket_name']}/{key}",
                                mimeType=content_type or "application/octet-stream"
                            )
                        )
                    ]
    except Exception as error:
        return [
            TextContent(
                type="text",
                text=f"Error: {str(error)}"
            )
        ]

async def main():
    # Run the server using stdin/stdout streams
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="s3-mcp-server",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())