from airflow.models import Pool, Variable, Connection
from airflow.settings import Session
from sqlalchemy.exc import IntegrityError

def setup_pools(session):
    """Tạo hoặc cập nhật các pools trong một session có sẵn."""
    print("--- Setting up Pools ---")
    pools_config = [
        {'pool': 'default_pool', 'slots': 128, 'description': 'Default pool', 'include_deferred': False},
        {'pool': 'db_pool', 'slots': 5, 'description': 'Pool for database connections', 'include_deferred': False},
        {'pool': 'crawl_pool', 'slots': 10, 'description': 'Pool for crawling tasks', 'include_deferred': False}
    ]
    
    for pool_config in pools_config:
        pool_name = pool_config['pool']
        existing_pool = session.query(Pool).filter(Pool.pool == pool_name).first()
        
        if existing_pool:
            print(f"Updating pool: {pool_name}")
            existing_pool.slots = pool_config['slots']
            existing_pool.description = pool_config['description']
            existing_pool.include_deferred = pool_config['include_deferred']
        else:
            print(f"Creating pool: {pool_name}")
            new_pool = Pool(**pool_config)
            session.add(new_pool)

def setup_connections(session):
    """Tạo hoặc cập nhật các connections trong một session có sẵn."""
    print("--- Setting up Connections ---")
    
    conn_id_to_create = "postgres_prod_db"
    conn_uri = "postgresql+psycopg2://postgres_user:postgres_password@postgres_db:5432/prod_db"
    
    # Logic "Delete and Create" để đảm bảo luôn cập nhật đúng
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id_to_create).first()
    if existing_conn:
        print(f"Deleting existing connection: {conn_id_to_create}")
        session.delete(existing_conn)
        # Commit ngay để đảm bảo việc xóa thành công trước khi tạo lại
        # Điều này tránh lỗi "unique constraint" nếu script chạy lại
        session.commit() 
    
    print(f"Creating connection: {conn_id_to_create}")
    new_conn = Connection(conn_id=conn_id_to_create, uri=conn_uri)
    session.add(new_conn)


def setup_variables(session):
    """Tạo hoặc cập nhật các variables trong một session có sẵn."""
    print("--- Setting up Variables ---")
    variables_config = {
        'tiki_batch_size': '20',
        'tiki_max_parallel': '5',
    }
    
    for key, value in variables_config.items():
        # Dùng set() của Variable class, không cần session
        Variable.set(key, value)
        print(f"Set variable: {key} = {value}")


if __name__ == "__main__":
    print("🚀 Starting Airflow setup...")
    
    # Tạo một session duy nhất để dùng cho tất cả các hàm
    db_session = Session()

    try:
        setup_pools(db_session)
        setup_connections(db_session)
        
        # Commit tất cả các thay đổi từ pools và connections
        db_session.commit()
        
        # Variables có cơ chế riêng, không cần session.commit()
        setup_variables(db_session)
        
        print("✅ Setup completed successfully!")

    except Exception as e:
        print(f"❌ An error occurred during setup: {e}")
        db_session.rollback() # Hoàn tác tất cả thay đổi nếu có lỗi
        raise # Ném lỗi ra để container biết là đã thất bại
    finally:
        db_session.close() # Luôn đóng session sau khi xong