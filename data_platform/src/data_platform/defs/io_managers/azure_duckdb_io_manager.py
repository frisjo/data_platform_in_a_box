import duckdb
import tempfile
import dagster as dg
from dagster import IOManager
from adlfs import AzureBlobFileSystem
from filelock import FileLock

""" IO Manager for duckdb in Azure blob storage """

class AzureDuckDBIOManager(IOManager):
    def __init__(self, account_name, account_key, container, database_path):
        self.account_name = account_name
        self.account_key = account_key
        self.container = container
        self.database_path = database_path

    def _get_fs(self):
        return AzureBlobFileSystem(account_name=self.account_name, account_key=self.account_key)

    def _get_remote_path(self):
        return f"{self.container}/{self.database_path}"

    def _get_local_path(self):
        return tempfile.mktemp(suffix=".duckdb")

    # Function to manage paralell runs
    # Creates a unique lock-file for tmp db uses filelock.FileLock to handle just one process at the time 
    def _acquire_lock(self):
        lock_file = f"/tmp/{self.database_path.replace('/', '_')}.lock"
        return FileLock(lock_file)

    def load_input(self, context):
        fs = self._get_fs()
        remote_path = self._get_remote_path()
        local_path = self._get_local_path()

        with self._acquire_lock():
            if fs.exists(remote_path):
                with fs.open(remote_path, "rb") as remote_file, open(local_path, "wb") as local_file:
                    local_file.write(remote_file.read())
            else:
                # Create empty file if db not exist in Azure blob storage
                conn = duckdb.connect(local_path)
                conn.close()

        conn = duckdb.connect(local_path)
        context.log.info(f"Loaded DuckDB from {remote_path}")
        # Return connection och tmp path for asset
        return conn, local_path

    def handle_output(self, context, obj):
        conn, local_path = obj
        conn.close()

        fs = self._get_fs()
        remote_path = self._get_remote_path()

        with self._acquire_lock():
            with open(local_path, "rb") as local_file, fs.open(remote_path, "wb") as remote_file:
                remote_file.write(local_file.read())

        context.log.info(f"Uploaded DuckDB to {remote_path}")

@dg.io_manager(config_schema={
    "account_name": str,
    "account_key": str,
    "container": str,
    "database_path": str,
})
def azure_duckdb_io_manager(init_context):
    return AzureDuckDBIOManager(
        account_name=init_context.resource_config["account_name"],
        account_key=init_context.resource_config["account_key"],
        container=init_context.resource_config["container"],
        database_path=init_context.resource_config["database_path"],
    )
