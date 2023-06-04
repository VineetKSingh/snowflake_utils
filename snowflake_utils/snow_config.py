import configparser
import os, sys

class SnowConfig:
    def __init__(self, **kwargs):
        self.user = kwargs.get("user")
        self.password = kwargs.get("password")
        self.account = kwargs.get("account")
        self.warehouse = kwargs.get("warehouse")
        self.database = kwargs.get("database")
        self.schema = kwargs.get("schema")
        self.role = kwargs.get("role")
        self.network_timeout = kwargs.get("network_timeout")
        self.login_timeout = kwargs.get("login_timeout")
        self.timezone = kwargs.get("timezone")
        self.autocommit = kwargs.get("autocommit")
        self.client_prefetch_threads = kwargs.get("client_prefetch_threads")

    @classmethod
    def default_config(cls, user, password, account):
        return cls(user=user, password=password, account=account)

    @classmethod
    def from_config_file(cls, config_file, environment='default'):
        # check if config_file is a valid file path if not, raise exception
        if not os.path.isfile(config_file):
            raise Exception("Invalid File Path")
        if not config_file.endswith(".ini"):
            raise Exception("Invalid File Type")
        config = configparser.ConfigParser()
        config.read(config_file)
        config_dict = dict(config[environment])
        if not config_dict.get("user") or not os.getenv("SNOWFLAKE_USER"):
            raise Exception("User is required. Add to config file as `user` or set `SNOWFLAKE_USER` as environment variable")
            sys.exit(1)
        if not config_dict.get("password") or not os.getenv("SNOWFLAKE_PASSWORD"):
            raise Exception("Password is required. Add to config file as `password` or set `SNOWFLAKE_PASSWORD` as environment variable")
            sys.exit(1)
        if not config_dict.get("account") or not os.getenv("SNOWFLAKE_ACCOUNT"):
            raise Exception("Account is required. Add to config file as `account` or set `SNOWFLAKE_ACCOUNT` as environment variable")
            sys.exit(1)
        return cls(**config_dict)
