"""
Turso 클라우드 데이터베이스에 테이블 생성

사용법:
1. .streamlit/secrets.toml 파일에 Turso 연결 정보 입력
2. python create_turso_schema.py 실행
"""

import libsql_experimental as libsql

# Turso 연결 정보 (여기에 직접 입력)
DATABASE_URL = "libsql://hoon-hoon.aws-ap-northeast-1.turso.io"
AUTH_TOKEN = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3Njg1MjM2NDcsImlkIjoiN2FjOTU0YTMtMThiNS00ZWI2LWJkOTQtMjI5ZWU3NzE1ZDFlIiwicmlkIjoiN2JiNjg3M2EtMTMxYy00ODg3LWFkYzktYjk0YmVmZDE5YzU3In0.yRArV2nfBwJFGEdGUyo79d-cjE6ZquVPea7FenL5pBDNK9bNgtPQ82fTc7cz80zfDprhJNgRpbk-CZZJUHxvCA"  # 메모장의 토큰 복사

def create_all_tables():
    """모든 테이블 생성"""
    
    print("=" * 70)
    print("Turso 클라우드 데이터베이스 스키마 생성")
    print("=" * 70)
    print()
    
    # 연결
    print("📡 Turso 연결 중...")
    conn = libsql.connect(database=DATABASE_URL, auth_token=AUTH_TOKEN)
    cursor = conn.cursor()
    print("✅ 연결 성공!")
    print()
    
    # 테이블 생성
    tables = [
        # 1. 생두 매입
        ("""
            CREATE TABLE IF NOT EXISTS green_bean_purchases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                purchase_date DATE NOT NULL,
                origin TEXT NOT NULL,
                product_name TEXT NOT NULL,
                quantity_kg REAL NOT NULL,
                unit_price REAL NOT NULL,
                total_amount REAL NOT NULL,
                supplier TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """, "green_bean_purchases"),
        
        # 2. 생두 재고
        ("""
            CREATE TABLE IF NOT EXISTS green_bean_inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bean_origin TEXT NOT NULL,
                bean_product TEXT NOT NULL,
                current_stock_kg REAL DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(bean_origin, bean_product)
            )
        """, "green_bean_inventory"),
        
        # 3. 배합비 (구 시스템)
        ("""
            CREATE TABLE IF NOT EXISTS blend_recipes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_name TEXT NOT NULL,
                green_bean_origin TEXT NOT NULL,
                green_bean_product TEXT NOT NULL,
                blend_ratio REAL NOT NULL,
                effective_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """, "blend_recipes"),
        
        # 4. 제품 판매
        ("""
            CREATE TABLE IF NOT EXISTS product_sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sale_date DATE NOT NULL,
                product_name TEXT NOT NULL,
                quantity_kg REAL NOT NULL,
                unit_price REAL NOT NULL,
                total_amount REAL NOT NULL,
                customer TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """, "product_sales"),
        
        # 5. 월별 변동비
        ("""
            CREATE TABLE IF NOT EXISTS variable_costs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                cost_per_kg REAL NOT NULL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(year, month)
            )
        """, "variable_costs"),
        
        # 6. 재고 이동 이력
        ("""
            CREATE TABLE IF NOT EXISTS inventory_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_date DATE NOT NULL,
                transaction_type TEXT NOT NULL,
                item_type TEXT NOT NULL,
                bean_origin TEXT,
                bean_product TEXT,
                quantity_kg REAL NOT NULL,
                reference_id INTEGER,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """, "inventory_transactions"),
        
        # 7. 대표 BOM (v3.1)
        ("""
            CREATE TABLE IF NOT EXISTS master_boms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bom_name TEXT UNIQUE NOT NULL,
                description TEXT,
                effective_date DATE NOT NULL,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """, "master_boms"),
        
        # 8. 대표 BOM 배합비 (v3.1)
        ("""
            CREATE TABLE IF NOT EXISTS master_bom_recipes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_bom_id INTEGER NOT NULL,
                green_bean_origin TEXT NOT NULL,
                green_bean_product TEXT NOT NULL,
                blend_ratio REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (master_bom_id) REFERENCES master_boms(id) ON DELETE CASCADE
            )
        """, "master_bom_recipes"),
        
        # 9. 제품 목록 (v3.1)
        ("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_name TEXT UNIQUE NOT NULL,
                master_bom_id INTEGER,
                is_active BOOLEAN DEFAULT 1,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (master_bom_id) REFERENCES master_boms(id) ON DELETE SET NULL
            )
        """, "products"),
        
        # 10. 제품-BOM 매칭 이력 (v3.1)
        ("""
            CREATE TABLE IF NOT EXISTS product_bom_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                master_bom_id INTEGER,
                effective_date DATE NOT NULL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
                FOREIGN KEY (master_bom_id) REFERENCES master_boms(id) ON DELETE SET NULL
            )
        """, "product_bom_history"),
    ]
    
    # 테이블 생성 실행
    for sql, name in tables:
        try:
            cursor.execute(sql)
            print(f"✅ {name:30} - 생성 완료")
        except Exception as e:
            print(f"❌ {name:30} - 오류: {str(e)}")
    
    # 인덱스 생성
    print()
    print("📊 인덱스 생성 중...")
    
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_green_purchases_date ON green_bean_purchases(purchase_date)",
        "CREATE INDEX IF NOT EXISTS idx_product_sales_date ON product_sales(sale_date)",
        "CREATE INDEX IF NOT EXISTS idx_blend_recipes_product ON blend_recipes(product_name)",
        "CREATE INDEX IF NOT EXISTS idx_master_bom_recipes_bom_id ON master_bom_recipes(master_bom_id)",
        "CREATE INDEX IF NOT EXISTS idx_products_bom_id ON products(master_bom_id)",
        "CREATE INDEX IF NOT EXISTS idx_products_name ON products(product_name)",
        "CREATE INDEX IF NOT EXISTS idx_product_bom_history_product_date ON product_bom_history(product_id, effective_date DESC)",
    ]
    
    for idx_sql in indexes:
        try:
            cursor.execute(idx_sql)
        except:
            pass
    
    print("✅ 인덱스 생성 완료")
    
    conn.commit()
    conn.close()
    
    print()
    print("=" * 70)
    print("✅ 모든 테이블 생성 완료!")
    print("=" * 70)
    print()
    print("🎉 Turso 데이터베이스 준비 완료!")
    print()
    print("다음 단계:")
    print("1. GitHub에 코드 업로드")
    print("2. Streamlit Cloud에서 배포")
    print("3. Streamlit Cloud Secrets에 연결 정보 입력")
    print()

if __name__ == "__main__":
    try:
        if AUTH_TOKEN == "여기에_실제_토큰을_붙여넣으세요":
            print("❌ 오류: AUTH_TOKEN을 입력해주세요!")
            print()
            print("방법:")
            print("1. 이 파일(create_turso_schema.py)을 텍스트 에디터로 열기")
            print("2. 11번째 줄의 AUTH_TOKEN = \"...\" 부분 찾기")
            print("3. 메모장에 저장한 실제 토큰으로 교체")
            print("4. 저장 후 다시 실행")
            print()
        else:
            create_all_tables()
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
