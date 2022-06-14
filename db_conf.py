import databases
import sqlalchemy


DATABASE_URL = "sqlite:///./test.db"
database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

address_table = sqlalchemy.Table(
    "address",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String),
    sqlalchemy.Column("latitude", sqlalchemy.Float),
    sqlalchemy.Column("longitude", sqlalchemy.Float),
    sqlalchemy.Column("is_deleted", sqlalchemy.Boolean, default=False, nullable=False),
)

