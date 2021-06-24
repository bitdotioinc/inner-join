import pandas as pd
import bitdotio
import psycopg2
from getpass import getpass
import os, io

"""This module provides a wrapper class to integrate bit.io with common Pandas dataframe operations."""

class BitDotIOPandas:
    """Wrapper class for Pandas and bit.io to make working with bit.io similar to local files.
    
    Attributes:
        api_key (str): A bit.io API key. Can be passed to constructor, read from ENV "BITDOTIO_API_KEY",
            or passed into the CLI, in that order of priority.
        username (str): Optional username to set. If not set, username must be specified for dependent op's.
        repo (str): Optional repo to set. If not set, repo must be specified for dependent op's.
    """
    # TODO(doss): Clean up propogation of exceptions through the stack and improve messages
    # TODO(doss): Write unittests once we have an API we agree on
    # TODO(doss): Add info messages to confirm successful DB write operations.
    
    # TODO: Handle more data types like time-zones, periods, half-precision, etc.
    DTYPE_MAP = {
        'object':'TEXT',
        'int16': 'INTEGER',
        'int32': 'INTEGER',
        'int64': 'INTEGER',
        'float16': 'REAL',
        'float32': 'REAL',
        'float64': 'REAL',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP WITHOUT TIME ZONE',
        'datetime64[ns, UTC]': 'TIMESTAMP WITH TIME ZONE',
        'datetime64[ns, US/Eastern]': 'TIMESTAMP WITH TIME ZONE'
    }
    
    def __init__(self, api_key=None, username=None, repo=None):
        if not api_key:
            api_key = self._get_api_key()
        # Test API key and raise exception if invalid
        try:
            self._b = bitdotio.bitdotio(api_key)
            self._connect()
        except Exception as e:
            raise ValueError("Unable to connect to bit.io with the provided API key.")
            
        # username and repo are optional at init
        self.username = username
        self.repo = repo
        
    def set_username(self, username):
        '''Sets repo username'''
        self.username = username
        
    def set_repo(self, repo):
        '''Sets repo'''
        self.repo = repo
        
    def list_tables(self, repo=None, username=None):
        '''Lists tables in a specified repo'''
        # TODO(doss): Add info about permissions, like ls -la
        username, repo = self._get_username_and_repo(username, repo)
        return [table.current_name for table in self._b.list_tables(username, repo)]
    
    def list_repos(self, username=None):
        '''Lists tables in a specified repo'''
        # TODO(doss): Add info about permissions, like ls -la
        username, _ = self._get_username_and_repo(username, None)
        return [table.name for table in self._b.list_repos(username)]
    
    def read_sql(self, sql):
        '''Query bit.io with SQL and return a pandas dataframe'''
        try:
            # Connect to bit.io
            conn = self._connect()
            # Execute sql
            return pd.read_sql(sql, conn)
        except Exception as e:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                
    def read_head(self, table, repo=None, username=None, limit=5):
        '''Get first "limit" rows from a table'''
        return self._read_table(table, repo, username, limit=limit)
                
    def read_table(self, table, repo=None, username=None, chunksize=None):
        '''Reads a table with optional pagination through a generator.
        
        Args:
            table (str): The table name in bit.io. If not provided, must be set in object.
            repo (str): The repo name in bit.io. If not provided, must be set in object.
            username (str): The username in bit.io. If not provided, must be set in object.
            chunksize (int): The maximum chunk size per download. If not provided the entire table
               is downloaded.
        Returns:
            A pandas DataFrame if no chunksize provided, else a generator that yields pandas
            DataFrames with a maximum of chunksize rows until all rows have been downloaded.
        '''
        if not chunksize:
            return self._read_table(table, repo, username)
        else:
            max_row = self._get_max_row(table, repo, username)
            n_chunks = (max_row // chunksize) + 1
            
            def table_chunk_gen():
                i = 0
                while i < n_chunks:
                    yield self._read_table(table, repo, username, limit=chunksize, offset=i * chunksize)
                    i += 1
            return table_chunk_gen()
        
    def delete_table(self, table, repo=None, username=None, limit=5):
        '''Deletes a table'''
        username, repo = self._get_username_and_repo(username, repo)
        self._validate_repo_and_table(repo, username, table)
        fully_qualified = self._get_fully_qualified(username, repo, table)
        self.sql(f'DROP TABLE {fully_qualified};')
        
    def sql(self, sql):
        '''Run arbitrary SQL statements on bitdotio'''
        try:
            # Connect to bit.io
            conn = self._connect()
            # Open cursor with bit.io server
            cur = conn.cursor()
            # Execute sql
            cur.execute(sql)
            # Close cursor
            cur.close()
            # Commit the changes (only relevent for write ops)
            conn.commit()
        except Exception as e:
            print(e)
        finally:
            if conn is not None:
                conn.close()

    def to_table(self, df, table, repo=None, username=None, append=True, chunksize=None):
        '''Write a dataframe to a bitdotio table, creating the table if necessary.

        One difference from a typical Pandas file operation is that we default to append,
        as a safer operation than truncate and insert (requires non-default argument).

        Args:
            df (Pandas DataFrame): The dataframe to upload.
            table (str): The table name in bit.io. If not provided, must be set in object.
            repo (str): The repo name in bit.io. If not provided, must be set in object.
            username (str): The username in bit.io. If not provided, must be set in object.
            append (str): Whether to append (default) or truncate and then insert. Optional.
            chunksize (int): Optional chunksize for uploading large tables, default None.
        '''
        # TODO(doss): This is a very naive implementation, maybe can use SQLAlchemy or our own ingestor 
        # TODO(doss): This should also support chunking for "big data" uploads
        username, repo = self._get_username_and_repo(username, repo)
        self._validate_repo(repo, username)
        fully_qualified = self._get_fully_qualified(username, repo, table)

        # Create table if needed
        if table not in self.list_tables(repo, username):
            self._create_table(username, repo, table, df)

        # Truncate if needed
        if not append:
            self.sql(f"DELETE FROM {fully_qualified};")
        # TODO(doss): look into more robust/performant implementation - SQLAlchemy?
        # TODO(doss): this implementation lacks integrity control for partial insert with chunking
        if chunksize is None:
            chunksize = df.shape[0]
        i = 0
        while i * chunksize < df.shape[0]:
            chunk_start, chunk_end = i * chunksize, (i + 1) * chunksize
            buffer = io.StringIO()
            df.iloc[chunk_start:chunk_end, :].to_csv(buffer, index=False, header=False, na_rep="null", line_terminator="\r\n")
            buffer.seek(0)
            try:
                conn = self._connect()
                cursor = conn.cursor()
                cursor.copy_expert(f"COPY {fully_qualified} FROM STDIN delimiter ',' null as 'null' csv;", buffer)
                conn.commit()
            except (Exception, psycopg2.DatabaseError) as e:
                print(e)
                conn.rollback()
            finally:
                if conn is not None:
                    conn.close()
            i += 1
        
    def __repr__(self):
        return f'BitDotIOPandas Object: username= {self.username}, repo= {self.repo}'
        
    def _get_username_and_repo(self, username, repo):
        '''Retrieves set username and repo if no arguments passed.'''
        username = username if username else self.username
        if not username:
            raise ValueError('Repo username must be initialized or provided as an argument.')
        repo = repo if repo else self.repo
        if not repo:
            raise ValueError('Repo name must be either initialized or provided as an argument.')
        return username, repo
    
    def _get_fully_qualified(self, username, repo, table):
        '''Constructs fully qualified table name from parts'''
        return f'"{username}/{repo}"."{table}"'

    def _get_api_key(self):
        '''Retrieves API key from ENV or username CLI input, in that order'''
        if os.getenv("BITDOTIO_API_KEY"):
            api_key = os.getenv("BITDOTIO_API_KEY")
        else:
            api_key = getpass("Please enter your bitdotio API key")
        return api_key
            
    def _connect(self):
        '''Gets a psycopg2 connection to bit.io'''
        # Get psycopg2 connection
        return self._b.get_connection()
    

    def _validate_repo(self, repo, username):
        '''Checks for repo'''
        # TODO: make this handle different permission levels later
        if repo not in self.list_repos(username):
            raise ValueError('Repo not found or not visible with your permissions. Try bpd.list_repos(repo, username).')
    
    def _validate_table(self, repo, username, table):
        '''Checks for repo'''
        # TODO: make this handle different permission levels later
        if table not in self.list_tables(repo, username):
            raise ValueError('Table not found or not visible with your permissions. Try bpd.list_tables(username).')
            
    def _validate_repo_and_table(self, repo, username, table):
        '''Validates repo and table'''
        self._validate_repo(repo, username)
        self._validate_table(repo, username, table)
            
    def _get_max_row(self, table, repo, username):
        '''Get maximum row number for a table'''
        username, repo = self._get_username_and_repo(username, repo)
        self._validate_repo_and_table(repo, username, table)
        fully_qualified = self._get_fully_qualified(username, repo, table)
        sql = f'SELECT COUNT(1) FROM {fully_qualified};'
        return self.read_sql(sql).values[0][0]
                 
    def _read_table(self, table, repo=None, username=None, limit=None, offset=None):
        '''Download from a table from bitdotio with optional limit and offset'''
        username, repo = self._get_username_and_repo(username, repo)
        self._validate_repo_and_table(repo, username, table)
        fully_qualified = self._get_fully_qualified(username, repo, table)
        sql = f'SELECT * FROM {fully_qualified};'
        # TODO: look into whether if full qualification and helper and int casting
        # are sufficient for secure use of this helper
        if limit:
            sql = sql[:-1] + f' LIMIT {int(limit)};'
        if offset:
            sql = sql[:-1] + f' OFFSET {int(offset)};'
        return self.read_sql(sql)
                
    def _create_table(self, username, repo, table, df):
        '''Automated table creation from a dataframe with limited type handling'''
        fully_qualified = self._get_fully_qualified(username, repo, table)
        sql = f'CREATE TABLE {fully_qualified} ('
        col_types = []
        breakpoint()
        for col, dtype in dict(df.dtypes).items():
            col_types.append(f'{col} {BitDotIOPandas.DTYPE_MAP.get(str(dtype), "TEXT")}')
        sql += ', '.join(col_types)
        sql += ')'
        self.sql(sql)
        return None