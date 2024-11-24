from dataclasses import dataclass

@dataclass
class DBCredentials:
    '''
    Container for database credentials
    '''
    supabase_url:str
    supabase_key:str
    db_host:str
    db_name:str
    db_user:str
    db_password:str
    db_port:int = 5432