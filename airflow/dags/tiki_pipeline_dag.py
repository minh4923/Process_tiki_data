from __future__ import annotations
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from typing import List, Dict
import logging

from utils.crawl_functions import (
    crawl_taxonomy_task, 
    save_taxonomy_task, 
    get_leaf_categories_batches, 
    crawl_products_batch, 
    save_products_batch
)

with DAG(
    dag_id='tiki_pipeline_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='A DAG to crawl and save Tiki products and taxonomy',
    schedule_interval=None, # Tạm thời tắt lịch chạy tự động để dễ test
    catchup=False,
    tags=['tiki', 'crawling', 'ecommerce'],
) as dag:

    @task
    def crawl_taxonomy() -> List[Dict]:
        """Task 1: Crawl all categories and return them as a list."""
        return crawl_taxonomy_task()

    @task
    def save_taxonomy(taxonomy_data: List[Dict]) -> int:
        """Task 2: Save the crawled categories to the database."""
        # Sửa tên tham số cho đúng
        return save_taxonomy_task(taxonomy_data=taxonomy_data, postgres_conn_id="postgres_prod_db")

    @task
    def prepare_product_batches(taxonomy_data: List[Dict]) -> List[List[Dict]]:
        """Task 3: Filter leaf categories and group them into batches for parallel processing."""
        # Sửa tên tham số cho đúng
        return get_leaf_categories_batches(taxonomy_data=taxonomy_data, batch_size=20)

    @task(pool='crawl_pool')
    def crawl_products_for_batch(category_batch: List[Dict]) -> List[Dict]:
        """Task 4 (dynamic): Crawl all products for a given batch of categories."""
        return crawl_products_batch(category_batch)

    @task(pool='db_pool')
    def save_products_for_batch(products_list: List[Dict]) -> int:
        """Task 5 (dynamic): Save a list of crawled products to the database."""
        # Tên tham số ở đây đã đúng
        return save_products_batch(products_list=products_list, postgres_conn_id="postgres_prod_db")
        
    @task
    def summary(saved_categories_count: int, saved_products_counts: list):
        """Task 6: Log a final summary of the pipeline run."""
        total_products_saved = sum(p for p in saved_products_counts if isinstance(p, int))
        logging.info("="*40)
        logging.info("PIPELINE SUMMARY")
        logging.info(f"    - Categories Saved/Updated: {saved_categories_count or 0}")
        logging.info(f"    - Total Products Saved/Updated: {total_products_saved}")
        logging.info("="*40)

    # --- Định nghĩa luồng công việc ---
    crawled_data = crawl_taxonomy()
    
    saved_tax_result = save_taxonomy(crawled_data)
    product_batches = prepare_product_batches(crawled_data)
    
    crawled_prods_result = crawl_products_for_batch.expand(category_batch=product_batches)
    saved_prods_result = save_products_for_batch.expand(products_list=crawled_prods_result)
    
    summary(saved_categories_count=saved_tax_result, saved_products_counts=saved_prods_result)