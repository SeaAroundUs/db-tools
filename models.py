from db import Base
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects import postgresql


class DataTransfer(Base):
    __tablename__ = 'datatransfer_tables'
    schema = 'admin'
    source_select_clause = "*"

    id = Column(Integer, primary_key=True)
    source_database_name = Column(String)
    source_table_name = Column(String)
    source_key_column = Column(String)
    source_select_clause = Column(String)
    source_where_clause = Column(String)
    target_schema_name = Column(String)
    target_table_name = Column(String)
    target_excluded_columns = Column(postgresql.ARRAY(String), default=[])
    number_of_threads = Column(Integer)


class AllocationResultPartitionMap(Base):
    __tablename__ = 'allocation_result_partition_map'
    schema = 'allocation'

    partition_id = Column(Integer, primary_key=True)
    begin_universal_data_id = Column(Integer, nullable=False)
    end_universal_data_id = Column(Integer, nullable=False)
    record_count = Column(Integer, nullable=False)
