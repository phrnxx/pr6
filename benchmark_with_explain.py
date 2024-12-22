import time
from sqlalchemy import create_engine, text
import pandas as pd
from db_config import get_database_url

def benchmark_operation(engine, operation_func, data_size):
    start_time = time.time()
    plan = operation_func(engine, data_size)
    end_time = time.time()
    return end_time - start_time, plan

def benchmark_select(engine, size):
    with engine.connect() as conn:
        plan = conn.execute(
            text("EXPLAIN ANALYZE SELECT * FROM users LIMIT :size"),
            {"size": size}
        ).fetchall()
        
        conn.execute(text("SELECT * FROM users LIMIT :size"), {"size": size})
        return plan

def benchmark_update(engine, size):
    with engine.connect() as conn:
        plan = conn.execute(
            text("""EXPLAIN ANALYZE 
                UPDATE users 
                SET name = name || '_updated' 
                WHERE id IN (SELECT id FROM users LIMIT :size)"""),
            {"size": size}
        ).fetchall()
        
        conn.execute(
            text("""UPDATE users 
                SET name = name || '_updated' 
                WHERE id IN (SELECT id FROM users LIMIT :size)"""),
            {"size": size}
        )
        conn.commit()
        return plan

def benchmark_delete(engine, size):
    with engine.connect() as conn:
        plan = conn.execute(
            text("""EXPLAIN ANALYZE 
                DELETE FROM users 
                WHERE id IN (SELECT id FROM users LIMIT :size)"""),
            {"size": size}
        ).fetchall()
        
        conn.execute(
            text("""DELETE FROM users 
                WHERE id IN (SELECT id FROM users LIMIT :size)"""),
            {"size": size}
        )
        conn.commit()
        return plan

def save_results(results, filename):
    df = pd.DataFrame(results)
    
    # Зберігаємо у CSV
    df.to_csv(f'{filename}.csv', index=False)
    
    # Зберігаємо у Markdown
    with open(f'{filename}.md', 'w', encoding='utf-8') as f:
        f.write("# Порівняльна таблиця результатів\n\n")
        f.write(df.to_markdown(index=False))

def run_benchmarks():
    engine = create_engine(get_database_url())
    data_sizes = [1000, 10000, 100000, 1000000]
    results = []

    for size in data_sizes:
        print(f"\nТестування {size} записів...")
        
        select_time, select_plan = benchmark_operation(engine, benchmark_select, size)
        update_time, update_plan = benchmark_operation(engine, benchmark_update, size)
        delete_time, delete_plan = benchmark_operation(engine, benchmark_delete, size)
        
        results.append({
            'Кількість записів': size,
            'Select (сек.)': round(select_time, 3),
            'Update (сек.)': round(update_time, 3),
            'Delete (сек.)': round(delete_time, 3)
        })
        
        # Виводимо плани виконання
        print("\nПлан виконання SELECT:")
        for row in select_plan:
            print(row[0])
            
        print("\nПлан виконання UPDATE:")
        for row in update_plan:
            print(row[0])
            
        print("\nПлан виконання DELETE:")
        for row in delete_plan:
            print(row[0])
    
    save_results(results, 'benchmark_results_with_indexes')

if __name__ == "__main__":
    run_benchmarks()