# S3 MCP Server

An MCP server for retrieving files from Amazon S3 — text, PDFs, images, and any other object type.

## Tools
- **ListBuckets**
  - Returns a list of all buckets owned by the authenticated sender of the request
- **ListObjectsV2**
  - Returns some or all (up to 1,000) of the objects in a bucket with each request
- **GetObject**
  - Retrieves an object from Amazon S3. Supports text and binary files.
  - `output_path` — save directly to disk instead of returning inline content
  - `max_bytes` — reject objects larger than this size before downloading
  - `extract_text` — extract text from PDFs instead of returning binary (requires `pymupdf`)
  - Every response includes metadata: `content_type`, `size_bytes`, `last_modified`
  - Text-based content types (json, csv, xml, etc.) are returned as plain text automatically
- **GetObjects**
  - Batch download multiple objects to a local directory in a single call
  - Provide explicit `keys` list or a `prefix` to list-and-download all matches
  - `max_bytes` — skip oversized files
  - Example: `GetObjects(bucket_name="b", prefix="35117/", output_dir="/tmp/35117/")`

#### Optional PDF text extraction

Install with `pip install 's3-mcp-server[pdf]'` to enable `extract_text=true` on GetObject.


## Configuration

### Setting up AWS Credentials
1. Obtain AWS access key ID, secret access key, and region from the AWS Management Console.
2. Ensure these credentials have appropriate permissions for AWS S3.

### Usage with Claude Desktop

#### Claude Desktop

On MacOS: `~/Library/Application\ Support/Claude/claude_desktop_config.json`
On Windows: `%APPDATA%/Claude/claude_desktop_config.json`

<details>
  <summary>Development/Unpublished Servers Configuration</summary>

```json
{
  "mcpServers": {
    "s3-mcp-server": {
      "command": "/absolute/path/to/mcp-server-s3/run.sh",
      "args": []
    }
  }
}
```

The `run.sh` wrapper automatically locates `uv` by searching common installation
paths (`~/.local/bin`, `~/.cargo/bin`, `/usr/local/bin`, `/opt/homebrew/bin`),
so it works even when the MCP client spawns the process with a limited `PATH`.

</details>

<details>
  <summary>Published Servers Configuration</summary>

```json
{
  "mcpServers": {
    "s3-mcp-server": {
      "command": "uvx",
      "args": [
        "s3-mcp-server"
      ]
    }
  }
}
  ```
</details>

## Development

### Building and Publishing

To prepare the package for distribution:

1. Sync dependencies and update lockfile:
```bash
uv sync
```

2. Build package distributions:
```bash
uv build
```

This will create source and wheel distributions in the `dist/` directory.

3. Publish to PyPI:
```bash
uv publish
```

Note: You'll need to set PyPI credentials via environment variables or command flags:
- Token: `--token` or `UV_PUBLISH_TOKEN`
- Or username/password: `--username`/`UV_PUBLISH_USERNAME` and `--password`/`UV_PUBLISH_PASSWORD`

### Debugging

Since MCP servers run over stdio, debugging can be challenging. For the best debugging
experience, we strongly recommend using the [MCP Inspector](https://github.com/modelcontextprotocol/inspector).


You can launch the MCP Inspector via [`npm`](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm) with this command:

```bash
npx @modelcontextprotocol/inspector /absolute/path/to/mcp-server-s3/run.sh
```


Upon launching, the Inspector will display a URL that you can access in your browser to begin debugging.


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.