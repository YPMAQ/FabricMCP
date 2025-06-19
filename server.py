# from FastMCP import MCPServer, tool
from FabricAPI import upload_file_to_lakehouse, download_file_from_lakehouse, list_items,get_token_via_browser
from mcp.server.fastmcp import FastMCP
import os
# Tool for uploading a file to lakehouse
app = FastMCP("FabricMCP")

@app.tool()
def upload_file_to_lakehouse_tool(account_name: str, WORKSPACE_NAME: str, local_file_path: str, lakehouse_file_path: str):
    """Upload a file to the Fabric Lakehouse."""
    upload_file_to_lakehouse(account_name, WORKSPACE_NAME, local_file_path, lakehouse_file_path)
    return f"Uploaded {local_file_path} to {lakehouse_file_path}"

# Tool for downloading a file from lakehouse (returns DataFrame as dict)
@app.tool()
def download_file_from_lakehouse_tool(ACCOUNT_NAME: str, WORKSPACE_NAME: str, lakehouse_file_path: str):
    """Download a CSV file from the Fabric Lakehouse and return as a list of dicts."""
    df = download_file_from_lakehouse(ACCOUNT_NAME, WORKSPACE_NAME, lakehouse_file_path)
    return df.to_dict(orient="records")

# Tool for listing items in a lakehouse directory
@app.tool()
def list_items_tool(ACCOUNT_NAME: str, WORKSPACE_NAME: str, DATA_PATH: str):
    """List items in a Fabric Lakehouse directory."""
    paths = list_items(ACCOUNT_NAME, WORKSPACE_NAME, DATA_PATH)
    return [p.name for p in paths]

@app.tool()
def get_token_via_browser_tool():
    """Generate an Azure token credential via browser login."""
    return get_token_via_browser()

if __name__ == "__main__":
    import asyncio
    # port = int(os.environ.get("PORT", 8000))
    asyncio.run(
        app.run_streamable_http_async(
            host="0.0.0.0",  # Changed from 127.0.0.1 to allow external connections
            port=8000,
            log_level="debug"
        )
    )