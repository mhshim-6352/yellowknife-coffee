import streamlit as st
import pandas as pd
from datetime import datetime, date
import time
import plotly.graph_objects as go
import plotly.express as px
import libsql_experimental as libsql

# ============================================
# 전역 상수 (한 곳에서만 관리!)
# ============================================
ROASTING_LOSS_RATE = 1.2  # 원두 1kg 생산 시 생두 1.2kg 필요

# 페이지 설정
st.set_page_config(
    page_title="Yellowknife 커피 재고 및 손익 관리 시스템",
    page_icon="☕",
    layout="wide"
)

# 데이터베이스 연결 함수
def get_db_connection():
    """Turso 클라우드 데이터베이스 연결"""
    try:
        # Streamlit Cloud에서는 secrets 사용
        database_url = st.secrets["turso"]["database_url"]
        auth_token = st.secrets["turso"]["auth_token"]
    except:
        # 로컬에서는 환경 변수 또는 기본값 사용
        import os
        database_url = os.getenv("TURSO_DATABASE_URL", "")
        auth_token = os.getenv("TURSO_AUTH_TOKEN", "")
    
    conn = libsql.connect(database=database_url, auth_token=auth_token)
    return conn


def execute_query_to_df(conn, query, params=None):
    """Turso 쿼리 결과를 DataFrame으로 변환"""
    try:
        if params:
            cursor = conn.execute(query, params)
        else:
            cursor = conn.execute(query)
        
        result = cursor.fetchall()
        
        if not result:
            return pd.DataFrame()
        
        # 컬럼명 추출
        if hasattr(cursor, 'description') and cursor.description:
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(result, columns=columns)
        else:
            return pd.DataFrame(result)
    except Exception as e:
        st.error(f"쿼리 실행 오류: {e}")
        return pd.DataFrame()

# ============================================
# 데이터베이스 초기화 (최초 1회 실행)
# ============================================

def initialize_database():
    """데이터베이스 초기 설정 - 모든 테이블 생성"""
    
    conn = get_db_connection()
    tables_sql = [
        """CREATE TABLE IF NOT EXISTS master_boms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bom_name TEXT UNIQUE NOT NULL,
            description TEXT,
            effective_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        
        """CREATE TABLE IF NOT EXISTS master_bom_recipes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            master_bom_id INTEGER NOT NULL,
            green_bean_origin TEXT NOT NULL,
            green_bean_product TEXT NOT NULL,
            blend_ratio REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (master_bom_id) REFERENCES master_boms(id) ON DELETE CASCADE
        )""",
        
        """CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT UNIQUE NOT NULL,
            master_bom_id INTEGER,
            is_active BOOLEAN DEFAULT 1,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (master_bom_id) REFERENCES master_boms(id) ON DELETE SET NULL
        )""",
        
        """CREATE TABLE IF NOT EXISTS product_bom_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            master_bom_id INTEGER,
            effective_date DATE NOT NULL,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
            FOREIGN KEY (master_bom_id) REFERENCES master_boms(id) ON DELETE SET NULL
        )""",
        
        """CREATE TABLE IF NOT EXISTS green_bean_purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            purchase_date DATE NOT NULL,
            origin TEXT NOT NULL,
            product_name TEXT NOT NULL,
            quantity_kg REAL NOT NULL,
            unit_price REAL NOT NULL,
            total_amount REAL NOT NULL,
            supplier TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        
        """CREATE TABLE IF NOT EXISTS green_bean_inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bean_origin TEXT NOT NULL,
            bean_product TEXT NOT NULL,
            current_stock_kg REAL DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(bean_origin, bean_product)
        )""",
        
        """CREATE TABLE IF NOT EXISTS product_sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_date DATE NOT NULL,
            product_name TEXT NOT NULL,
            quantity_kg REAL NOT NULL,
            unit_price REAL NOT NULL,
            total_amount REAL NOT NULL,
            customer TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        
        """CREATE TABLE IF NOT EXISTS blend_recipes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            green_bean_origin TEXT NOT NULL,
            green_bean_product TEXT NOT NULL,
            blend_ratio REAL NOT NULL,
            effective_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        
        """CREATE TABLE IF NOT EXISTS variable_costs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            cost_per_kg REAL NOT NULL,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(year, month)
        )""",
        
        """CREATE TABLE IF NOT EXISTS inventory_transactions (
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
        )"""
    ]
    
    indexes_sql = [
        "CREATE INDEX IF NOT EXISTS idx_green_purchases_date ON green_bean_purchases(purchase_date)",
        "CREATE INDEX IF NOT EXISTS idx_product_sales_date ON product_sales(sale_date)",
        "CREATE INDEX IF NOT EXISTS idx_blend_recipes_product ON blend_recipes(product_name)",
        "CREATE INDEX IF NOT EXISTS idx_master_bom_recipes_bom_id ON master_bom_recipes(master_bom_id)",
        "CREATE INDEX IF NOT EXISTS idx_products_bom_id ON products(master_bom_id)",
        "CREATE INDEX IF NOT EXISTS idx_products_name ON products(product_name)",
        "CREATE INDEX IF NOT EXISTS idx_product_bom_history_product_date ON product_bom_history(product_id, effective_date DESC)"
    ]
    
    try:
        # 테이블 생성
        for sql in tables_sql:
            conn.execute(sql)
        
        # 인덱스 생성
        for sql in indexes_sql:
            conn.execute(sql)
        
        conn.commit()
        return True, "✅ 데이터베이스 초기화 완료!"
        
    except Exception as e:
        return False, f"❌ 오류: {str(e)}"
    finally:
        conn.close()

# ============================================
# 재고 관리 헬퍼 함수들
# ============================================

def execute_to_dataframe(query, params=None):
    """Turso에서 쿼리 실행 후 DataFrame 반환 (컬럼명 포함)"""
    conn = get_db_connection()
    try:
        if params:
            cursor_result = conn.execute(query, params)
        else:
            cursor_result = conn.execute(query)
        
        # 결과 가져오기
        rows = cursor_result.fetchall()
        
        # 컬럼명 추출 (description 사용)
        try:
            columns = [desc[0] for desc in cursor_result.description]
        except:
            # description이 없으면 기본 컬럼명
            columns = None
        
        # DataFrame 생성
        if columns:
            df = pd.DataFrame(rows, columns=columns)
        else:
            df = pd.DataFrame(rows)
        
        return df
    finally:
        conn.close()



def get_bean_full_name(origin, product):
    """원산지 + 제품명 조합"""
    return f"{origin} - {product}" if product else origin

def update_green_bean_inventory(origin, product, quantity_change):
    """생두 재고 업데이트 (원산지 + 제품명)"""
    conn = get_db_connection()
    # 현재 재고 확인
    conn.execute("""
        SELECT current_stock_kg FROM green_bean_inventory 
        WHERE bean_origin = ? AND bean_product = ?
    """, (origin, product))
    result = conn.execute("""
        SELECT current_stock_kg FROM green_bean_inventory 
        WHERE bean_origin = ? AND bean_product = ?
    """, (origin, product)).fetchone()
    
    if result:
        # 기존 재고 업데이트
        new_stock = result[0] + quantity_change
        conn.execute("""
            UPDATE green_bean_inventory 
            SET current_stock_kg = ?, last_updated = CURRENT_TIMESTAMP
            WHERE bean_origin = ? AND bean_product = ?
        """, (new_stock, origin, product))
    else:
        # 새로운 생두 추가
        conn.execute("""
            INSERT INTO green_bean_inventory (bean_origin, bean_product, current_stock_kg)
            VALUES (?, ?, ?)
        """, (origin, product, quantity_change))
    
    conn.commit()
    conn.close()

def add_inventory_transaction(transaction_date, transaction_type, item_type, 
                              origin, product, quantity_kg, reference_id=None, notes=""):
    """재고 이동 이력 추가"""
    conn = get_db_connection()
    conn.execute("""
        INSERT INTO inventory_transactions 
        (transaction_date, transaction_type, item_type, bean_origin, bean_product,
         quantity_kg, reference_id, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (transaction_date, transaction_type, item_type, origin, product,
          quantity_kg, reference_id, notes))
    
    conn.commit()
    conn.close()

def get_bean_stock(origin, product):
    """생두 재고 조회"""
    conn = get_db_connection()
    conn.execute("""
        SELECT current_stock_kg FROM green_bean_inventory 
        WHERE bean_origin = ? AND bean_product = ?
    """, (origin, product))
    result = conn.execute("""
        SELECT current_stock_kg FROM green_bean_inventory 
        WHERE bean_origin = ? AND bean_product = ?
    """, (origin, product)).fetchone()
    conn.close()
    return result[0] if result else 0

# ============================================
# 새로운 BOM 관리 시스템 헬퍼 함수들
# ============================================

def get_master_bom_recipe(master_bom_id):
    """대표 BOM의 배합비 조회"""
    conn = get_db_connection()
    conn.execute("""
        SELECT green_bean_origin, green_bean_product, blend_ratio
        FROM master_bom_recipes
        WHERE master_bom_id = ?
    """, (master_bom_id,))
    recipes = conn.execute("""
        SELECT green_bean_origin, green_bean_product, blend_ratio
        FROM master_bom_recipes
        WHERE master_bom_id = ?
    """, (master_bom_id,)).fetchall()
    conn.close()
    return recipes

def get_product_bom(product_name, sale_date=None):
    """제품명으로 배합비 조회 (새 시스템 우선, 없으면 구 시스템)"""
    conn = get_db_connection()
    # 1. 새 시스템: 제품 → 제품-BOM 이력 → 대표 BOM → 배합비
    # 제품 ID 조회
    conn.execute(
        "SELECT id FROM products WHERE product_name = ? AND is_active = 1",
        (product_name,)
    )
    product_result = conn.execute(
        "SELECT id FROM products WHERE product_name = ? AND is_active = 1",
        (product_name,)
    ).fetchone()
    
    if product_result:
        product_id = product_result[0]
        
        # 날짜별 BOM 이력 조회
        if sale_date:
            conn.execute(
                "SELECT master_bom_id, effective_date FROM product_bom_history "
                "WHERE product_id = ? AND effective_date <= ? "
                "ORDER BY effective_date DESC LIMIT 1",
                (product_id, sale_date)
            )
        else:
            # 날짜 없으면 가장 최근 이력 조회
            conn.execute(
                "SELECT master_bom_id, effective_date FROM product_bom_history "
                "WHERE product_id = ? ORDER BY effective_date DESC LIMIT 1",
                (product_id,)
            )
        
            bom_result = conn.execute(
                "SELECT master_bom_id, effective_date FROM product_bom_history "
                "WHERE product_id = ? ORDER BY effective_date DESC LIMIT 1",
                (product_id,)
            ).fetchone()
        
        if bom_result and bom_result[0]:
            # 해당 날짜의 대표 BOM 배합비 조회
            master_bom_id = bom_result[0]
            conn.execute(
                "SELECT green_bean_origin, green_bean_product, blend_ratio "
                "FROM master_bom_recipes WHERE master_bom_id = ?",
                (master_bom_id,)
            )
            recipes = conn.execute(
                "SELECT green_bean_origin, green_bean_product, blend_ratio "
                "FROM master_bom_recipes WHERE master_bom_id = ?",
                (master_bom_id,)
            ).fetchall()
            conn.close()
            return recipes, 'new_system'
    
    # 2. 구 시스템: blend_recipes 테이블 조회 (날짜별)
    if sale_date:
        conn.execute(
            "SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date "
            "FROM blend_recipes WHERE product_name = ? "
            "AND (effective_date IS NULL OR effective_date <= ?) "
            "ORDER BY effective_date DESC",
            (product_name, sale_date)
        )
    else:
        conn.execute(
            "SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date "
            "FROM blend_recipes WHERE product_name = ? ORDER BY effective_date DESC",
            (product_name,)
        )
    
        all_recipes = conn.execute(
            "SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date "
            "FROM blend_recipes WHERE product_name = ? ORDER BY effective_date DESC",
            (product_name,)
        ).fetchall()
    conn.close()
    
    if all_recipes:
        # 가장 최근 적용일의 배합비만 사용
        latest_effective_date = all_recipes[0][3]
        recipes = [(r[0], r[1], r[2]) for r in all_recipes if r[3] == latest_effective_date]
        return recipes, 'old_system'
    
    return [], 'none'

def get_all_master_boms():
    """모든 대표 BOM 목록 조회"""
    conn = get_db_connection()
    df = execute_to_dataframe("""
        SELECT id, bom_name, description, effective_date, is_active
        FROM master_boms
        ORDER BY bom_name
    """)
    conn.close()
    return df

def get_all_products():
    """모든 제품 목록 조회 (최신 BOM 이력 포함)"""
    conn = get_db_connection()
    df = execute_to_dataframe("""
        SELECT 
            p.id, 
            p.product_name,
            p.master_bom_id,
            m.bom_name,
            p.is_active, 
            p.notes,
            (SELECT effective_date FROM product_bom_history 
             WHERE product_id = p.id 
             ORDER BY effective_date DESC LIMIT 1) as latest_bom_date
        FROM products p
        LEFT JOIN master_boms m ON p.master_bom_id = m.id
        ORDER BY p.product_name
    """)
    conn.close()
    return df

# ============================================
# 데이터베이스 초기화 함수
# ============================================

def initialize_database():
    """데이터베이스 초기 설정 - 모든 테이블 생성"""
    
    conn = get_db_connection()
    tables_sql = [
        """CREATE TABLE IF NOT EXISTS master_boms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bom_name TEXT UNIQUE NOT NULL,
            description TEXT,
            effective_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        
        """CREATE TABLE IF NOT EXISTS master_bom_recipes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            master_bom_id INTEGER NOT NULL,
            green_bean_origin TEXT NOT NULL,
            green_bean_product TEXT NOT NULL,
            blend_ratio REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (master_bom_id) REFERENCES master_boms(id) ON DELETE CASCADE
        )""",
        
        """CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT UNIQUE NOT NULL,
            master_bom_id INTEGER,
            is_active BOOLEAN DEFAULT 1,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (master_bom_id) REFERENCES master_boms(id) ON DELETE SET NULL
        )""",
        
        """CREATE TABLE IF NOT EXISTS product_bom_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            master_bom_id INTEGER,
            effective_date DATE NOT NULL,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
            FOREIGN KEY (master_bom_id) REFERENCES master_boms(id) ON DELETE SET NULL
        )""",
        
        """CREATE TABLE IF NOT EXISTS green_bean_purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            purchase_date DATE NOT NULL,
            origin TEXT NOT NULL,
            product_name TEXT NOT NULL,
            quantity_kg REAL NOT NULL,
            unit_price REAL NOT NULL,
            total_amount REAL NOT NULL,
            supplier TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        
        """CREATE TABLE IF NOT EXISTS green_bean_inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bean_origin TEXT NOT NULL,
            bean_product TEXT NOT NULL,
            current_stock_kg REAL DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(bean_origin, bean_product)
        )""",
        
        """CREATE TABLE IF NOT EXISTS product_sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_date DATE NOT NULL,
            product_name TEXT NOT NULL,
            quantity_kg REAL NOT NULL,
            unit_price REAL NOT NULL,
            total_amount REAL NOT NULL,
            customer TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        
        """CREATE TABLE IF NOT EXISTS blend_recipes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            green_bean_origin TEXT NOT NULL,
            green_bean_product TEXT NOT NULL,
            blend_ratio REAL NOT NULL,
            effective_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        
        """CREATE TABLE IF NOT EXISTS variable_costs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            cost_per_kg REAL NOT NULL,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(year, month)
        )""",
        
        """CREATE TABLE IF NOT EXISTS inventory_transactions (
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
        )"""
    ]
    
    indexes_sql = [
        "CREATE INDEX IF NOT EXISTS idx_green_purchases_date ON green_bean_purchases(purchase_date)",
        "CREATE INDEX IF NOT EXISTS idx_product_sales_date ON product_sales(sale_date)",
        "CREATE INDEX IF NOT EXISTS idx_blend_recipes_product ON blend_recipes(product_name)",
        "CREATE INDEX IF NOT EXISTS idx_master_bom_recipes_bom_id ON master_bom_recipes(master_bom_id)",
        "CREATE INDEX IF NOT EXISTS idx_products_bom_id ON products(master_bom_id)",
        "CREATE INDEX IF NOT EXISTS idx_products_name ON products(product_name)",
        "CREATE INDEX IF NOT EXISTS idx_product_bom_history_product_date ON product_bom_history(product_id, effective_date DESC)"
    ]
    
    try:
        for sql in tables_sql:
            conn.execute(sql)
        for sql in indexes_sql:
            conn.execute(sql)
        conn.commit()
        return True, "✅ 데이터베이스 초기화 완료!"
    except Exception as e:
        return False, f"❌ 오류: {str(e)}"
    finally:
        conn.close()



# ============================================
# 메인 앱
# ============================================

st.title("☕ Yellowknife 커피 재고 및 손익 관리 시스템")
st.markdown("---")

# 사이드바 메뉴
menu = st.sidebar.selectbox(
    "메뉴 선택",
    ["📥 데이터 입력", "✏️ 데이터 수정/삭제", "📊 데이터 조회 및 분석", 
     "📦 재고 관리", "💰 손익 분석", "🔬 배합 계산기"]
)

# ============================================
# 📥 데이터 입력 메뉴
# ============================================
if menu == "📥 데이터 입력":
    st.header("📥 데이터 입력")
    
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "생두 매입", 
        "🆕 대표 BOM 관리", 
        "🆕 제품 관리",
        "🆕 제품-BOM 매칭",
        "제품 판매 (엑셀 업로드)", 
        "월별 변동비"
    ])
    
    # 생두 매입 입력
    with tab1:
        st.subheader("🌱 생두 매입 입력")
        
        col1, col2 = st.columns(2)
        with col1:
            purchase_date = st.date_input("매입 날짜", date.today(), key="purchase_date")
            purchase_date = purchase_date.strftime('%Y-%m-%d') if purchase_date else None
            bean_origin = st.text_input("생두 원산지", placeholder="예: 브라질")
            bean_product = st.text_input("생두 제품명", placeholder="예: 브라질 15/16")
            quantity = st.number_input("수량 (kg)", min_value=0.0, step=0.1)
        with col2:
            unit_price = st.number_input("단가 (원/kg)", min_value=0.0, step=100.0)
            supplier = st.text_input("공급처")
        
        if st.button("생두 매입 등록", key="btn_purchase"):
            if bean_origin and bean_product and quantity > 0 and unit_price > 0:
                total = quantity * unit_price
                conn = get_db_connection()
                conn.execute("""
                    INSERT INTO green_bean_purchases 
                    (purchase_date, origin, product_name, quantity_kg, unit_price, total_amount, supplier)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (purchase_date, bean_origin, bean_product, quantity, unit_price, total, supplier))
                
                purchase_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                conn.commit()
                conn.close()
                
                # 재고 업데이트
                update_green_bean_inventory(bean_origin, bean_product, quantity)
                
                # 재고 이동 이력 추가
                add_inventory_transaction(
                    purchase_date, 'bean_purchase', 'green_bean', 
                    bean_origin, bean_product, quantity, purchase_id, 
                    f"매입 - {supplier}"
                )
                
                st.toast("✅ 등록 완료!", icon="✅")
                st.success(f"✅ 생두 매입 등록 완료! (총액: {total:,.0f}원)")
                st.success(f"📦 {get_bean_full_name(bean_origin, bean_product)} 재고 {quantity}kg 증가")
                time.sleep(1)  # 메시지 표시
                st.rerun()
            else:
                st.error("⚠️ 모든 필수 항목을 입력해주세요.")
    
    # 🆕 대표 BOM 관리
    with tab2:
        st.subheader("🆕 대표 BOM 관리")
        st.info("💡 대표 BOM은 여러 제품이 공유하는 배합비입니다. 한 번 등록하면 여러 제품에서 재사용할 수 있습니다.")
        
        # 기존 대표 BOM 목록 표시
        st.markdown("### 📋 등록된 대표 BOM")
        master_boms_df = get_all_master_boms()
        
        if len(master_boms_df) > 0:
            st.dataframe(master_boms_df, use_container_width=True)
            
            # 선택한 BOM의 상세 배합비 보기
            if len(master_boms_df) > 0:
                st.markdown("### 🔍 배합비 상세 보기")
                selected_bom_name = st.selectbox(
                    "대표 BOM 선택",
                    master_boms_df['bom_name'].tolist(),
                    key="view_bom"
                )
                
                if selected_bom_name:
                    selected_bom_id = master_boms_df[master_boms_df['bom_name'] == selected_bom_name]['id'].iloc[0]
                    recipes = get_master_bom_recipe(selected_bom_id)
                    
                    if recipes:
                        recipe_df = pd.DataFrame(recipes, columns=['원산지', '제품명', '배합비(%)'])
                        recipe_df['생두'] = recipe_df.apply(lambda row: f"{row['원산지']} - {row['제품명']}", axis=1)
                        st.dataframe(recipe_df[['생두', '배합비(%)']], use_container_width=True)
                        st.info(f"합계: {recipe_df['배합비(%)'].sum():.1f}%")
        else:
            st.info("등록된 대표 BOM이 없습니다. 아래에서 새로 등록해주세요.")
        
        st.markdown("---")
        st.markdown("### ➕ 새 대표 BOM 등록")
        
        col_name, col_date = st.columns([2, 1])
        with col_name:
            bom_name = st.text_input("대표 BOM 이름", placeholder="예: Grosso Blend", key="new_bom_name")
        with col_date:
            effective_date = st.date_input("적용 시작일", date.today(), key="bom_effective_date")
            effective_date = effective_date.strftime('%Y-%m-%d') if effective_date else None
        
        description = st.text_area("설명 (선택사항)", placeholder="예: 기본 블렌드 배합", key="bom_description")
        
        st.markdown("##### 배합비 입력 (합계 100%)")
        num_beans = st.number_input("사용할 생두 종류 수", min_value=1, max_value=10, value=2, key="num_beans_bom")
        
        blend_data = []
        total_ratio = 0
        
        for i in range(num_beans):
            st.markdown(f"**생두 {i+1}**")
            col1, col2, col3 = st.columns(3)
            with col1:
                origin = st.text_input(f"원산지", key=f"bom_origin_{i}", placeholder="예: 브라질")
            with col2:
                product = st.text_input(f"제품명", key=f"bom_product_{i}", placeholder="예: 브라질 15/16")
            with col3:
                ratio = st.number_input(f"비율 (%)", min_value=0.0, max_value=100.0, 
                                       step=0.1, key=f"bom_ratio_{i}")
            if origin and product and ratio > 0:
                blend_data.append((origin, product, ratio))
                total_ratio += ratio
        
        st.info(f"현재 합계: {total_ratio:.1f}%")
        
        if st.button("대표 BOM 등록", key="btn_register_bom"):
            if not bom_name:
                st.error("⚠️ 대표 BOM 이름을 입력해주세요.")
            elif abs(total_ratio - 100) > 0.01:
                st.error(f"⚠️ 배합비 합계가 100%가 아닙니다. (현재: {total_ratio:.1f}%)")
            elif len(blend_data) == 0:
                st.error("⚠️ 최소 1개 이상의 생두를 입력해주세요.")
            else:
                conn = get_db_connection()
                try:
                    # 대표 BOM 등록
                    conn.execute("""
                        INSERT INTO master_boms (bom_name, description, effective_date)
                        VALUES (?, ?, ?)
                    """, (bom_name, description, effective_date))
                    
                    master_bom_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                    
                    # 배합비 등록
                    for origin, product, ratio in blend_data:
                        conn.execute("""
                            INSERT INTO master_bom_recipes 
                            (master_bom_id, green_bean_origin, green_bean_product, blend_ratio)
                            VALUES (?, ?, ?, ?)
                        """, (master_bom_id, origin, product, ratio))
                    
                    conn.commit()
                    st.toast("✅ 등록 완료!", icon="✅")
                    st.success(f"✅ 대표 BOM '{bom_name}' 등록 완료!")
                    time.sleep(1)  # 메시지 표시
                    st.rerun()
                    
                except sqlite3.IntegrityError:
                    st.error(f"⚠️ '{bom_name}' 이름은 이미 사용 중입니다. 다른 이름을 입력해주세요.")
                finally:
                    conn.close()
    
    # 🆕 제품 관리
    with tab3:
        st.subheader("🆕 제품 관리")
        st.info("💡 실제 판매하는 제품을 등록합니다. ERP 엑셀에서 자동으로 추출할 수도 있습니다.")
        
        # 기존 제품 목록
        st.markdown("### 📋 등록된 제품 목록")
        products_df = get_all_products()
        
        if len(products_df) > 0:
            # BOM 연결 상태 표시
            products_df['BOM 연결'] = products_df['bom_name'].fillna('❌ 미연결')
            display_df = products_df[['product_name', 'BOM 연결', 'is_active']].copy()
            display_df.columns = ['제품명', '연결된 대표 BOM', '판매중']
            
            st.dataframe(display_df, use_container_width=True)
            st.caption(f"총 {len(products_df)}개 제품 등록됨")
        else:
            st.info("등록된 제품이 없습니다.")
        
        st.markdown("---")
        
        # 제품 등록 방법 선택
        add_method = st.radio(
            "제품 등록 방법",
            ["개별 입력", "ERP 엑셀 일괄 등록"],
            key="product_add_method"
        )
        
        if add_method == "개별 입력":
            st.markdown("### ➕ 제품 개별 등록")
            
            product_name = st.text_input("제품명", placeholder="예: Grosso 1kg", key="new_product_name")
            notes = st.text_area("비고 (선택사항)", key="product_notes")
            
            if st.button("제품 등록", key="btn_add_product"):
                if product_name:
                    conn = get_db_connection()
                    try:
                        conn.execute("""
                            INSERT INTO products (product_name, notes)
                            VALUES (?, ?)
                        """, (product_name, notes))
                        conn.commit()
                        st.toast("✅ 등록 완료!", icon="✅")
                        st.success(f"✅ 제품 '{product_name}' 등록 완료!")
                        time.sleep(1)  # 메시지 표시
                        st.rerun()
                    except sqlite3.IntegrityError:
                        st.error(f"⚠️ '{product_name}'은(는) 이미 등록된 제품입니다.")
                    finally:
                        conn.close()
                else:
                    st.error("⚠️ 제품명을 입력해주세요.")
        
        else:  # ERP 엑셀 일괄 등록
            st.markdown("### 📁 ERP 엑셀에서 제품 일괄 추출")
            st.warning("⚠️ 엑셀의 제품명 컬럼에서 중복을 제거하여 자동으로 제품을 등록합니다.")
            
            uploaded_file = st.file_uploader(
                "ERP 엑셀 파일 선택 (판매 데이터)", 
                type=['xlsx', 'xls'],
                key="product_excel"
            )
            
            if uploaded_file:
                try:
                    # 엑셀 읽기
                    df = pd.read_excel(uploaded_file, header=1)  # ERP 형식 (2행이 헤더)
                    st.success(f"✅ 파일 읽기 완료: {len(df)}개 행")
                    
                    # 제품명 컬럼 선택
                    product_col = st.selectbox("제품명 컬럼 선택", df.columns, key="product_col_select")
                    
                    if st.button("제품 추출 및 등록", key="btn_extract_products"):
                        # 중복 제거
                        unique_products = df[product_col].dropna().unique()
                        st.info(f"📊 추출된 제품 수: {len(unique_products)}개")
                        
                        # 미리보기
                        with st.expander("추출된 제품 목록 미리보기"):
                            st.write(pd.DataFrame(unique_products, columns=['제품명']))
                        
                        # 등록
                        conn = get_db_connection()
                        success_count = 0
                        skip_count = 0
                        
                        for product_name in unique_products:
                            try:
                                conn.execute("""
                                    INSERT INTO products (product_name)
                                    VALUES (?)
                                """, (str(product_name),))
                                success_count += 1
                            except sqlite3.IntegrityError:
                                # 이미 존재하는 제품은 건너뛰기
                                skip_count += 1
                        
                        conn.commit()
                        conn.close()
                        
                        st.toast("✅ 등록 완료!", icon="✅")
                        st.success(f"✅ 제품 등록 완료! (신규: {success_count}개, 기존: {skip_count}개)")
                        time.sleep(1)  # 메시지 표시
                        st.rerun()
                        
                except Exception as e:
                    st.error(f"❌ 파일 처리 오류: {str(e)}")
    
    # 🆕 제품-BOM 매칭
    with tab4:
        st.subheader("🆕 제품-BOM 매칭")
        st.info("💡 등록된 제품에 대표 BOM을 연결합니다. 같은 BOM을 사용하는 제품을 한번에 선택할 수 있습니다.")
        
        products_df = get_all_products()
        master_boms_df = get_all_master_boms()
        
        if len(products_df) == 0:
            st.warning("⚠️ 등록된 제품이 없습니다. 먼저 '제품 관리' 탭에서 제품을 등록해주세요.")
        elif len(master_boms_df) == 0:
            st.warning("⚠️ 등록된 대표 BOM이 없습니다. 먼저 '대표 BOM 관리' 탭에서 BOM을 등록해주세요.")
        else:
            # 매칭 상태 요약
            matched_count = len(products_df[products_df['master_bom_id'].notna()])
            unmatched_count = len(products_df) - matched_count
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("전체 제품", len(products_df))
            with col2:
                st.metric("BOM 연결됨", matched_count)
            with col3:
                st.metric("BOM 미연결", unmatched_count)
            
            st.markdown("---")
            
            # 매칭 방법 선택
            match_method = st.radio(
                "매칭 방법",
                ["개별 매칭", "일괄 매칭 (엑셀)"],
                key="match_method"
            )
            
            if match_method == "개별 매칭":
                st.markdown("### 🔗 제품-BOM 개별 매칭")
                
                # 제품 선택
                product_names = products_df['product_name'].tolist()
                selected_products = st.multiselect(
                    "제품 선택 (여러 개 선택 가능)",
                    product_names,
                    key="selected_products_match"
                )
                
                if selected_products:
                    # BOM 선택
                    bom_options = ["연결 해제"] + master_boms_df['bom_name'].tolist()
                    selected_bom = st.selectbox(
                        f"연결할 대표 BOM ({len(selected_products)}개 제품)",
                        bom_options,
                        key="selected_bom_match"
                    )
                    
                    # 선택한 BOM의 배합비 미리보기
                    if selected_bom != "연결 해제":
                        bom_id = master_boms_df[master_boms_df['bom_name'] == selected_bom]['id'].iloc[0]
                        recipes = get_master_bom_recipe(bom_id)
                        
                        with st.expander(f"'{selected_bom}' 배합비 미리보기"):
                            recipe_df = pd.DataFrame(recipes, columns=['원산지', '제품명', '배합비(%)'])
                            st.dataframe(recipe_df, use_container_width=True)
                    
                    # 🆕 적용 시작일 입력
                    st.markdown("---")
                    effective_date_match = st.date_input(
                        "적용 시작일",
                        date.today(),
                        help="이 날짜부터 새로운 BOM이 적용됩니다. 과거 판매 데이터는 과거 BOM을 사용합니다.",
                        key="effective_date_match"
                    )
                    effective_date_match = effective_date_match.strftime('%Y-%m-%d') if effective_date_match else None
                    
                    notes_match = st.text_input(
                        "비고 (선택사항)",
                        placeholder="예: 배합비 변경, 신제품 출시 등",
                        key="notes_match"
                    )
                    
                    if st.button("매칭 적용", key="btn_apply_match"):
                        conn = get_db_connection()
                        if selected_bom == "연결 해제":
                            # BOM 연결 해제 (이력은 유지)
                            for product in selected_products:
                                conn.execute(
                                    "UPDATE products SET master_bom_id = NULL WHERE product_name = ?",
                                    (product,)
                                )
                            conn.commit()
                            conn.close()
                            st.success(f"✅ {len(selected_products)}개 제품의 BOM 연결이 해제되었습니다.")
                            st.info("💡 과거 매칭 이력은 유지되므로, 과거 판매 데이터의 원가 계산은 정상 작동합니다.")
                        else:
                            # BOM 연결 및 이력 추가
                            bom_id = master_boms_df[master_boms_df['bom_name'] == selected_bom]['id'].iloc[0]
                            
                            for product in selected_products:
                                # 1. products 테이블 업데이트 (최신 BOM ID)
                                conn.execute(
                                    "UPDATE products SET master_bom_id = ? WHERE product_name = ?",
                                    (bom_id, product)
                                )
                                
                                # 2. 제품 ID 조회
                                conn.execute(
                                    "SELECT id FROM products WHERE product_name = ?",
                                    (product,)
                                )
                                product_id = conn.execute(
                                    "SELECT id FROM products WHERE product_name = ?",
                                    (product,)
                                ).fetchone()
                                
                                # 3. 이력 추가
                                conn.execute(
                                    "INSERT INTO product_bom_history "
                                    "(product_id, master_bom_id, effective_date, notes) "
                                    "VALUES (?, ?, ?, ?)",
                                    (product_id, bom_id, effective_date_match, notes_match)
                                )
                            
                            conn.commit()
                            conn.close()
                            st.toast("✅ 완료!", icon="✅")
                            st.success(f"✅ {len(selected_products)}개 제품이 '{selected_bom}'에 연결되었습니다!")
                            st.success(f"📅 적용 시작일: {effective_date_match}")
                            st.info("💡 이 날짜 이전 판매 데이터는 이전 BOM을 사용하고, 이후는 새 BOM을 사용합니다.")
                        
                        time.sleep(1)  # 메시지 표시
                        st.rerun()
            
            else:  # 일괄 매칭 (엑셀)
                st.markdown("### 📁 엑셀로 일괄 매칭")
                st.info("💡 엑셀 형식: [제품명] [대표BOM이름] [적용일] 3개 컬럼")
                
                # 샘플 다운로드
                sample_df = pd.DataFrame({
                    '제품명': products_df['product_name'].head(5).tolist(),
                    '대표BOM이름': ['Grosso Blend'] * 5,
                    '적용일': [date.today().strftime('%Y-%m-%d')] * 5
                })
                
                with st.expander("📋 엑셀 템플릿 다운로드"):
                    st.dataframe(sample_df)
                    st.download_button(
                        label="템플릿 다운로드",
                        data=sample_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'),
                        file_name="제품_BOM_매칭_템플릿.csv",
                        mime="text/csv"
                    )
                
                uploaded_match_file = st.file_uploader(
                    "매칭 엑셀 파일 업로드",
                    type=['xlsx', 'xls', 'csv'],
                    key="match_excel"
                )
                
                if uploaded_match_file:
                    try:
                        if uploaded_match_file.name.endswith('.csv'):
                            match_df = pd.read_csv(uploaded_match_file)
                        else:
                            match_df = pd.read_excel(uploaded_match_file)
                        
                        st.write("업로드된 매칭 데이터:")
                        st.dataframe(match_df.head(10))
                        
                        if st.button("일괄 매칭 적용", key="btn_bulk_match"):
                            conn = get_db_connection()
                            success_count = 0
                            error_messages = []
                            
                            for idx, row in match_df.iterrows():
                                try:
                                    product_name = str(row[match_df.columns[0]])
                                    bom_name = str(row[match_df.columns[1]])
                                    
                                    # 적용일 처리 (3번째 컬럼, 없으면 오늘)
                                    if len(match_df.columns) >= 3 and pd.notna(row[match_df.columns[2]]):
                                        effective_date_str = str(row[match_df.columns[2]])
                                        effective_date_bulk = pd.to_datetime(effective_date_str).date()
                                    else:
                                        effective_date_bulk = date.today()
                                    
                                    # BOM ID 조회
                                    bom_result = master_boms_df[master_boms_df['bom_name'] == bom_name]
                                    
                                    if len(bom_result) == 0:
                                        error_messages.append(f"행 {idx+2}: BOM '{bom_name}'을 찾을 수 없습니다.")
                                        continue
                                    
                                    bom_id = bom_result['id'].iloc[0]
                                    
                                    # 제품 조회 및 업데이트
                                    conn.execute(
                                        "SELECT id FROM products WHERE product_name = ?",
                                        (product_name,)
                                    )
                                    product_result = conn.execute(
                                        "SELECT id FROM products WHERE product_name = ?",
                                        (product_name,)
                                    ).fetchone()
                                    
                                    if not product_result:
                                        error_messages.append(f"행 {idx+2}: 제품 '{product_name}'을 찾을 수 없습니다.")
                                        continue
                                    
                                    product_id = product_result[0]
                                    
                                    # 1. products 테이블 업데이트
                                    conn.execute(
                                        "UPDATE products SET master_bom_id = ? WHERE id = ?",
                                        (bom_id, product_id)
                                    )
                                    
                                    # 2. 이력 추가
                                    conn.execute(
                                        "INSERT INTO product_bom_history "
                                        "(product_id, master_bom_id, effective_date, notes) "
                                        "VALUES (?, ?, ?, ?)",
                                        (product_id, bom_id, effective_date_bulk, "일괄 매칭")
                                    )
                                    
                                    success_count += 1
                                    
                                except Exception as e:
                                    error_messages.append(f"행 {idx+2}: 오류 - {str(e)}")
                            
                            conn.commit()
                            conn.close()
                            
                            st.success(f"✅ 일괄 매칭 완료! (성공: {success_count}개)")
                            
                            if error_messages:
                                with st.expander(f"⚠️ 오류 {len(error_messages)}건"):
                                    for msg in error_messages[:10]:
                                        st.write(msg)
                            
                            time.sleep(1)  # 메시지 표시
                            st.rerun()
                            
                    except Exception as e:
                        st.error(f"❌ 파일 처리 오류: {str(e)}")
    
    # 제품 판매 엑셀 업로드 (기존 코드 유지, 배합비 조회만 수정)
    with tab5:
        st.subheader("📦 제품 판매 데이터 업로드 (ERP 엑셀)")
        
        st.info("💡 부가세(VAT)가 포함된 단가는 자동으로 제거됩니다 (단가 ÷ 1.1)")
        st.success(f"✨ 판매 시 자동으로 생두 차감! (원두 1kg = 생두 {ROASTING_LOSS_RATE}kg)")
        st.warning("⚠️ ERP 엑셀 파일의 경우, 자동으로 올바른 헤더를 찾아 처리합니다.")
        
        uploaded_file = st.file_uploader("엑셀 파일 선택", type=['xlsx', 'xls'], key="sales_excel")
        
        if uploaded_file:
            try:
                # 엑셀 헤더 자동 감지
                df_test = pd.read_excel(uploaded_file, header=0, nrows=3)
                
                if '일자' in df_test.columns or '품명 및 규격' in df_test.columns:
                    header_row = 0
                    header_info = "엑셀 1행"
                elif 'Unnamed' in str(df_test.columns[0]) or '회사명' in str(df_test.columns[0]):
                    header_row = 1
                    header_info = "엑셀 2행"
                else:
                    header_row = 0
                    header_info = "엑셀 1행"
                
                df = pd.read_excel(uploaded_file, header=header_row)
                df = df.dropna(how='all')
                
                st.success(f"✅ {header_info}을 헤더로 인식했습니다.")
                st.write(f"📋 업로드된 데이터 미리보기:")
                st.dataframe(df.head())
                st.info(f"📊 총 {len(df)}개 행의 데이터가 감지되었습니다.")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    date_col = st.selectbox("날짜 컬럼", df.columns, key="sales_date_col")
                with col2:
                    product_col = st.selectbox("제품명 컬럼", df.columns, key="sales_product_col")
                with col3:
                    qty_col = st.selectbox("수량 컬럼", df.columns, key="sales_qty_col")
                with col4:
                    price_col = st.selectbox("단가 컬럼", df.columns, key="sales_price_col")
                
                customer_col = st.selectbox("거래처 컬럼 (선택사항)", ["없음"] + list(df.columns), key="sales_customer_col")
                
                if st.button("데이터 업로드 및 생두 자동 차감", key="btn_upload_sales"):
                    conn = get_db_connection()
                    success_count = 0
                    error_count = 0
                    warning_messages = []
                    
                    for idx, row in df.iterrows():
                        try:
                            if pd.isna(row[date_col]) or pd.isna(row[product_col]):
                                continue
                            
                            # 날짜 파싱
                            sale_date_str = str(row[date_col]).strip()
                            if ' -' in sale_date_str or ' +' in sale_date_str:
                                sale_date_str = sale_date_str.split(' ')[0]
                            elif '-' in sale_date_str and '/' in sale_date_str:
                                sale_date_str = sale_date_str.split('-')[0].strip()
                            
                            sale_date = pd.to_datetime(sale_date_str).date()
                            product = str(row[product_col])
                            quantity = float(row[qty_col])
                            unit_price_with_vat = float(row[price_col])
                            unit_price = unit_price_with_vat / 1.1
                            total = quantity * unit_price
                            customer = str(row[customer_col]) if customer_col != "없음" else ""
                            
                            # 🔧 수정: 새로운 get_product_bom 함수 사용
                            recipe, system_type = get_product_bom(product, sale_date)
                            
                            if not recipe:
                                warning_messages.append(f"⚠️ 행 {idx+2}: {product}의 배합비가 없습니다. 생두 차감 없이 판매만 기록됩니다.")
                            else:
                                # 생두 차감 로직
                                green_bean_needed = round(quantity * ROASTING_LOSS_RATE, 3)
                                insufficient_beans = []
                                
                                for origin, product_name_bean, ratio in recipe:
                                    required_qty = round(green_bean_needed * (ratio / 100), 3)
                                    current_stock = get_bean_stock(origin, product_name_bean)
                                    
                                    if current_stock < required_qty:
                                        insufficient_beans.append(
                                            f"{get_bean_full_name(origin, product_name_bean)} (필요: {required_qty:.1f}kg, 현재: {current_stock:.1f}kg)"
                                        )
                                
                                if insufficient_beans:
                                    warning_messages.append(
                                        f"⚠️ 행 {idx+2}: {product} {quantity}kg - 생두 재고 부족! {', '.join(insufficient_beans)}"
                                    )
                                else:
                                    for origin, product_name_bean, ratio in recipe:
                                        required_qty = round(green_bean_needed * (ratio / 100), 3)
                                        update_green_bean_inventory(origin, product_name_bean, -required_qty)
                                        add_inventory_transaction(
                                            sale_date, 'sale', 'green_bean',
                                            origin, product_name_bean, -required_qty, None,
                                            f"{product} {quantity}kg 판매 → 생두 {green_bean_needed}kg 사용 ({customer})"
                                        )
                            
                            # 판매 데이터 입력
                            conn.execute("""
                                INSERT INTO product_sales 
                                (sale_date, product_name, quantity_kg, unit_price, total_amount, customer)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (sale_date, product, quantity, unit_price, total, customer))
                            
                            success_count += 1
                            
                        except Exception as e:
                            error_count += 1
                            warning_messages.append(f"❌ 행 {idx+2} 처리 오류: {str(e)}")
                    
                    conn.commit()
                    conn.close()
                    
                    st.success(f"✅ 업로드 완료! (성공: {success_count}건, 실패: {error_count}건)")
                    
                    if warning_messages:
                        st.warning(f"⚠️ 경고 메시지 {len(warning_messages)}건:")
                        for msg in warning_messages[:10]:
                            st.write(msg)
                        if len(warning_messages) > 10:
                            st.write(f"... 외 {len(warning_messages)-10}개 메시지")
                    
            except Exception as e:
                st.error(f"❌ 파일 처리 오류: {str(e)}")
    
    # 월별 변동비 입력 (기존 코드 유지)
    with tab6:
        st.subheader("💰 월별 변동비 입력")
        st.info("💡 전기세, 수도세, 가스비, 임차료, 인건비 등을 입력하세요.")
        
        col1, col2 = st.columns(2)
        with col1:
            year = st.number_input("연도", min_value=2020, max_value=2030, value=date.today().year, key="cost_year")
            month = st.number_input("월", min_value=1, max_value=12, value=date.today().month, key="cost_month")
        
        st.markdown("##### 비용 항목")
        col1, col2, col3 = st.columns(3)
        with col1:
            electricity = st.number_input("전기세", min_value=0.0, step=1000.0, key="electricity")
            water = st.number_input("수도세", min_value=0.0, step=1000.0, key="water")
        with col2:
            gas = st.number_input("가스비", min_value=0.0, step=1000.0, key="gas")
            rent = st.number_input("임차료", min_value=0.0, step=10000.0, key="rent")
        with col3:
            labor = st.number_input("인건비", min_value=0.0, step=100000.0, key="labor")
            other = st.number_input("기타", min_value=0.0, step=1000.0, key="other")
        
        total_cost = electricity + water + gas + rent + labor + other
        st.info(f"합계: {total_cost:,.0f}원")
        
        if st.button("월별 변동비 등록", key="btn_cost"):
            conn = get_db_connection()
            # 기존 데이터 확인
            conn.execute("""
                SELECT id FROM monthly_costs WHERE year = ? AND month = ?
            """, (year, month))
            existing = conn.execute("""
                SELECT id FROM monthly_costs WHERE year = ? AND month = ?
            """, (year, month)).fetchone()
            
            if existing:
                # 업데이트
                conn.execute("""
                    UPDATE monthly_costs 
                    SET electricity = ?, water = ?, gas = ?, rent = ?, labor = ?, other = ?
                    WHERE year = ? AND month = ?
                """, (electricity, water, gas, rent, labor, other, year, month))
                st.success(f"✅ {year}년 {month}월 변동비 업데이트 완료!")
            else:
                # 신규 등록
                conn.execute("""
                    INSERT INTO monthly_costs (year, month, electricity, water, gas, rent, labor, other)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (year, month, electricity, water, gas, rent, labor, other))
                st.toast("✅ 등록 완료!", icon="✅")
                st.success(f"✅ {year}년 {month}월 변동비 등록 완료!")
            
            conn.commit()
            conn.close()
            time.sleep(1)  # 메시지 표시
            st.rerun()

# 나머지 메뉴들은 기존 코드와 동일하게 유지
# (데이터 수정/삭제, 데이터 조회, 재고 관리, 손익 분석, 배합 계산기)

# ✏️ 데이터 수정/삭제 메뉴 (간소화)
# ============================================
elif menu == "✏️ 데이터 수정/삭제":
    st.header("✏️ 데이터 수정 및 삭제")
    st.info("💡 주요 데이터 수정/삭제 기능입니다. 재고 이력은 자동으로 기록됩니다.")
    
    tab1, tab2, tab3 = st.tabs(["생두 매입", "배합비", "판매 데이터"])
    
    # 생두 매입 수정/삭제
    with tab1:
        st.subheader("🌱 생두 매입 수정/삭제")
        
        st.info("💡 수정할 데이터를 선택하면 자동으로 값이 입력됩니다.")
        
        conn = get_db_connection()
        purchases_df = execute_to_dataframe("""
            SELECT id, purchase_date, origin, product_name, quantity_kg, 
                   unit_price, total_amount, supplier
            FROM green_bean_purchases
            ORDER BY purchase_date DESC
            LIMIT 50
        """)
        conn.close()
        
        if len(purchases_df) > 0:
            # 테이블 표시
            st.dataframe(purchases_df, use_container_width=True)
            
            # 🔧 새로운 UI: 드롭다운으로 선택
            purchase_options = [
                f"ID {row['id']} | {row['purchase_date']} | {row['origin']} - {row['product_name']} | {row['quantity_kg']}kg"
                for _, row in purchases_df.iterrows()
            ]
            
            # session_state로 선택 상태 관리
            if 'selected_purchase_id' not in st.session_state:
                st.session_state.selected_purchase_id = purchases_df.iloc[0]['id']
            
            selected_display = st.selectbox(
                "수정/삭제할 데이터 선택",
                purchase_options,
                key="purchase_selector"
            )
            
            # 선택된 ID 추출
            selected_id = int(selected_display.split('|')[0].replace('ID', '').strip())
            
            # 선택이 변경되었는지 감지
            if selected_id != st.session_state.selected_purchase_id:
                st.session_state.selected_purchase_id = selected_id
                time.sleep(1)  # 메시지 표시
                st.rerun()
            
            # 선택된 데이터 가져오기
            record = purchases_df[purchases_df['id'] == selected_id].iloc[0]
            
            # 2컬럼 레이아웃
            col_left, col_right = st.columns([1, 1])
            
            with col_left:
                st.markdown("##### 📋 현재 데이터")
                st.write(f"**ID:** {record['id']}")
                st.write(f"**날짜:** {record['purchase_date']}")
                st.write(f"**원산지:** {record['origin']}")
                st.write(f"**제품명:** {record['product_name']}")
                st.write(f"**수량:** {record['quantity_kg']} kg")
                st.write(f"**단가:** {record['unit_price']:,.0f} 원/kg")
                st.write(f"**총액:** {record['total_amount']:,.0f} 원")
                st.write(f"**공급처:** {record['supplier']}")
            
            with col_right:
                st.markdown("##### ✏️ 수정하기")
                
                # 수정 입력 폼 (자동으로 현재 값 채움)
                new_date = st.date_input(
                    "날짜",
                    value=pd.to_datetime(record['purchase_date']).date(),
                    key=f"edit_purchase_date_{selected_id}"
                )
                new_date = new_date.strftime('%Y-%m-%d') if new_date else None
                
                new_origin = st.text_input(
                    "원산지",
                    value=record['origin'],
                    key=f"edit_purchase_origin_{selected_id}"
                )
                
                new_product = st.text_input(
                    "제품명",
                    value=record['product_name'],
                    key=f"edit_purchase_product_{selected_id}"
                )
                
                new_quantity = st.number_input(
                    "수량 (kg)",
                    value=float(record['quantity_kg']),
                    min_value=0.0,
                    step=0.1,
                    key=f"edit_purchase_qty_{selected_id}"
                )
                
                new_unit_price = st.number_input(
                    "단가 (원/kg)",
                    value=float(record['unit_price']),
                    min_value=0.0,
                    step=100.0,
                    key=f"edit_purchase_price_{selected_id}"
                )
                
                new_supplier = st.text_input(
                    "공급처",
                    value=record['supplier'] if record['supplier'] else "",
                    key=f"edit_purchase_supplier_{selected_id}"
                )
                
                st.info(f"수정 후 총액: {new_quantity * new_unit_price:,.0f} 원")
                
                # 수정/삭제 버튼
                col_edit, col_delete = st.columns(2)
                
                with col_edit:
                    if st.button("✅ 수정 적용", key="purchase_edit_btn", type="primary"):
                        if new_origin and new_product and new_quantity > 0 and new_unit_price > 0:
                            new_total = new_quantity * new_unit_price
                            
                            conn = get_db_connection()
                            conn.execute("""
                                UPDATE green_bean_purchases
                                SET purchase_date=?, origin=?, product_name=?, 
                                    quantity_kg=?, unit_price=?, total_amount=?, supplier=?
                                WHERE id=?
                            """, (new_date, new_origin, new_product, new_quantity, 
                                  new_unit_price, new_total, new_supplier, selected_id))
                            conn.commit()
                            conn.close()
                            
                            st.toast("✅ 수정 완료!", icon="✅")
                            st.success("✅ 수정 완료!")
                            time.sleep(1)  # 메시지 표시
                            st.rerun()
                        else:
                            st.error("⚠️ 모든 필수 항목을 입력해주세요.")
                
                with col_delete:
                    if st.button("⚠️ 삭제하기", key="purchase_delete_btn", type="secondary"):
                        conn = get_db_connection()
                        
                        # 1단계: 삭제할 데이터 조회 (재고 차감용)
                        purchase_data = conn.execute("""
                            SELECT origin, product_name, quantity_kg
                            FROM green_bean_purchases
                            WHERE id = ?
                        """, (selected_id,)).fetchone()
                        
                        if purchase_data:
                            origin, product_name, quantity = purchase_data
                            
                            # 2단계: 재고 차감
                            conn.execute("""
                                UPDATE green_bean_inventory
                                SET current_stock_kg = current_stock_kg - ?,
                                    last_updated = CURRENT_TIMESTAMP
                                WHERE bean_origin = ? AND bean_product = ?
                            """, (quantity, origin, product_name))
                            
                            # 3단계: 재고 이동 이력 기록
                            conn.execute("""
                                INSERT INTO inventory_transactions
                                (transaction_date, transaction_type, item_type, bean_origin, bean_product, 
                                 quantity_kg, reference_id, notes)
                                VALUES (date('now'), 'purchase_delete', 'green_bean', ?, ?, ?, ?, 
                                        '매입 데이터 삭제로 인한 재고 차감')
                            """, (origin, product_name, -quantity, selected_id))
                            
                            # 4단계: 매입 데이터 삭제
                            conn.execute("DELETE FROM green_bean_purchases WHERE id=?", (selected_id,))
                            conn.commit()
                            conn.close()
                            
                            # session_state 초기화
                            if 'selected_purchase_id' in st.session_state:
                                del st.session_state.selected_purchase_id
                            
                            st.toast("✅ 삭제 완료!", icon="✅")
                            st.success("✅ 매입 데이터 삭제 완료!")
                            st.success(f"📦 {product_name} 재고 {quantity}kg 차감")
                            time.sleep(1)
                            st.rerun()
                        else:
                            conn.close()
                            st.error("삭제할 데이터를 찾을 수 없습니다.")
        else:
            st.info("등록된 생두 매입 데이터가 없습니다.")
    
    # 배합비 수정/삭제
    with tab2:
        st.subheader("🧪 배합비 수정/삭제")
        
        st.info("💡 배합비를 수정하거나 삭제할 수 있습니다. 제품을 선택하면 자동으로 현재 배합비가 입력됩니다.")
        
        conn = get_db_connection()
        recipes_df = execute_to_dataframe("""
            SELECT product_name, 
                   GROUP_CONCAT(green_bean_origin || ' - ' || green_bean_product || ' (' || blend_ratio || '%)') as recipe
            FROM blend_recipes
            GROUP BY product_name
        """)
        conn.close()
        
        if len(recipes_df) > 0:
            st.dataframe(recipes_df)
            
            # 🔧 수정: session_state로 제품 변경 감지
            if 'selected_product_for_edit' not in st.session_state:
                st.session_state.selected_product_for_edit = recipes_df['product_name'].tolist()[0]
            
            product_to_edit = st.selectbox(
                "수정/삭제할 제품", 
                recipes_df['product_name'].tolist(),
                key="product_selector"
            )
            
            # 제품이 변경되었는지 감지
            if product_to_edit != st.session_state.selected_product_for_edit:
                st.session_state.selected_product_for_edit = product_to_edit
                # 강제 리렌더링으로 새 값 로드
                time.sleep(1)  # 메시지 표시
                st.rerun()
            
            # 선택한 제품의 현재 배합비 조회
            conn = get_db_connection()
            current_recipe = execute_to_dataframe("""
                SELECT green_bean_origin, green_bean_product, blend_ratio
                FROM blend_recipes
                WHERE product_name = ?
                ORDER BY blend_ratio DESC
            """, conn, params=(product_to_edit,))
            conn.close()
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("##### 📋 현재 배합비")
                st.dataframe(current_recipe)
            
            with col2:
                st.markdown("##### ✏️ 수정하기")
                st.info(f"💡 {product_to_edit}의 새로운 배합비를 입력하세요 (합계 100%)")
                
                # 적용일자 입력 추가
                new_effective_date = st.date_input(
                    "새 배합비 적용 시작일",
                    date.today(),
                    key=f"edit_effective_date_{product_to_edit}",
                    help="이 날짜부터 새로운 배합비가 적용됩니다"
                )
                new_effective_date = new_effective_date.strftime('%Y-%m-%d') if new_effective_date else None
                
                num_beans = st.number_input("사용할 생두 종류 수", min_value=1, max_value=10, 
                                            value=len(current_recipe), key="edit_num_beans")
                
                new_blend_data = []
                total_ratio = 0
                
                for i in range(num_beans):
                    col_a, col_b, col_c = st.columns(3)
                    with col_a:
                        default_origin = current_recipe.iloc[i]['green_bean_origin'] if i < len(current_recipe) else ""
                        origin = st.text_input(
                            f"원산지 {i+1}", 
                            value=default_origin,
                            key=f"edit_origin_{i}_{product_to_edit}"  # 🔧 제품별로 고유한 키
                        )
                    with col_b:
                        default_product = current_recipe.iloc[i]['green_bean_product'] if i < len(current_recipe) else ""
                        product = st.text_input(
                            f"제품명 {i+1}", 
                            value=default_product,
                            key=f"edit_product_{i}_{product_to_edit}"  # 🔧 제품별로 고유한 키
                        )
                    with col_c:
                        default_ratio = float(current_recipe.iloc[i]['blend_ratio']) if i < len(current_recipe) else 0.0
                        ratio = st.number_input(
                            f"비율 (%)", 
                            min_value=0.0, 
                            max_value=100.0, 
                            value=default_ratio,
                            step=0.1, 
                            key=f"edit_ratio_{i}_{product_to_edit}"  # 🔧 제품별로 고유한 키
                        )
                    
                    if origin and product and ratio > 0:
                        new_blend_data.append((origin, product, ratio))
                        total_ratio += ratio
                
                st.info(f"현재 합계: {total_ratio:.1f}%")
                
                col_edit, col_delete = st.columns(2)
                
                with col_edit:
                    if st.button("✅ 수정 적용", key="blend_edit", type="primary"):
                        if abs(total_ratio - 100) > 0.01:
                            st.error(f"⚠️ 배합비 합계가 100%가 아닙니다. (현재: {total_ratio:.1f}%)")
                        elif len(new_blend_data) == 0:
                            st.error("⚠️ 최소 1개 이상의 생두를 입력해주세요.")
                        else:
                            conn = get_db_connection()
                            # 🔧 수정: 기존 배합비는 삭제하지 않고 새 버전 추가 (이력 관리)
                            
                            # 새 배합비 입력 (적용일자 포함)
                            for origin, product, ratio in new_blend_data:
                                conn.execute("""
                                    INSERT INTO blend_recipes (product_name, effective_date, green_bean_origin, green_bean_product, blend_ratio)
                                    VALUES (?, ?, ?, ?, ?)
                                """, (product_to_edit, new_effective_date, origin, product, ratio))
                            
                            conn.commit()
                            conn.close()
                            st.toast("✅ 수정 완료!", icon="✅")
                            st.success(f"✅ {product_to_edit} 배합비 수정 완료! ({new_effective_date}부터 적용)")
                            time.sleep(1)  # 메시지 표시
                            st.rerun()  # 🔧 수정: 페이지 자동 새로고침
                
                with col_delete:
                    if st.button("⚠️ 배합비 전체 삭제", key="blend_delete", type="secondary"):
                        conn = get_db_connection()
                        conn.execute("DELETE FROM blend_recipes WHERE product_name=?", (product_to_edit,))
                        conn.commit()
                        conn.close()
                        st.toast("✅ 삭제 완료!", icon="✅")
                        st.success(f"✅ {product_to_edit} 배합비 삭제 완료!")
                        # session_state 초기화
                        if 'selected_product_for_edit' in st.session_state:
                            del st.session_state.selected_product_for_edit
                        time.sleep(1)  # 메시지 표시
                        st.rerun()  # 🔧 수정: 페이지 자동 새로고침
        else:
            st.info("등록된 배합비가 없습니다.")
    
    # 판매 데이터 수정/삭제
    with tab3:
        st.subheader("📦 판매 데이터 수정/삭제")
        
        st.info("💡 수정할 판매 데이터를 선택하면 자동으로 값이 입력됩니다.")
        st.warning("⚠️ 삭제 시 차감된 생두 재고가 자동으로 복원됩니다.")
        
        conn = get_db_connection()
        sales_df = execute_query_to_df(conn, """
            SELECT id, sale_date, product_name, quantity_kg, 
                   unit_price, total_amount, customer
            FROM product_sales
            ORDER BY sale_date DESC
            LIMIT 100
        """)
        conn.close()
        
        if len(sales_df) > 0:
            # 테이블 표시
            st.dataframe(sales_df, use_container_width=True)
            
            # 드롭다운 옵션 생성
            sales_options = [
                f"ID {row['id']} | {row['sale_date']} | {row['product_name']} | {row['quantity_kg']}kg | {row['customer']}"
                for _, row in sales_df.iterrows()
            ]
            
            # session_state로 선택 상태 관리
            if 'selected_sale_id' not in st.session_state:
                st.session_state.selected_sale_id = sales_df.iloc[0]['id']
            
            selected_display = st.selectbox(
                "수정/삭제할 판매 데이터 선택",
                sales_options,
                key="sale_selector"
            )
            
            # 선택된 ID 추출
            selected_id = int(selected_display.split('|')[0].replace('ID', '').strip())
            
            # 선택이 변경되었는지 감지
            if selected_id != st.session_state.selected_sale_id:
                st.session_state.selected_sale_id = selected_id
                time.sleep(1)  # 메시지 표시
                st.rerun()
            
            # 선택된 데이터 가져오기
            record = sales_df[sales_df['id'] == selected_id].iloc[0]
            
            # 2컬럼 레이아웃
            col_left, col_right = st.columns([1, 1])
            
            with col_left:
                st.markdown("##### 📋 현재 데이터")
                st.write(f"**ID:** {record['id']}")
                st.write(f"**판매일:** {record['sale_date']}")
                st.write(f"**제품명:** {record['product_name']}")
                st.write(f"**수량:** {record['quantity_kg']} kg")
                st.write(f"**단가:** {record['unit_price']:,.0f} 원/kg")
                st.write(f"**총액:** {record['total_amount']:,.0f} 원")
                st.write(f"**거래처:** {record['customer']}")
                
                # 사용된 배합비 확인
                conn = get_db_connection()
                conn.execute("""
                    SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                    FROM blend_recipes
                    WHERE product_name = ?
                    AND (effective_date IS NULL OR effective_date <= ?)
                    ORDER BY effective_date DESC
                """, (record['product_name'], record['sale_date']))
                
                recipe_records = conn.execute("""
                    SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                    FROM blend_recipes
                    WHERE product_name = ?
                    AND (effective_date IS NULL OR effective_date <= ?)
                    ORDER BY effective_date DESC
                """, (record['product_name'], record['sale_date'])).fetchall()
                conn.close()
                
                if recipe_records:
                    latest_date = recipe_records[0][3]
                    current_recipe = [r for r in recipe_records if r[3] == latest_date]
                    
                    st.markdown("**사용된 배합비:**")
                    for r in current_recipe:
                        st.write(f"- {r[0]} - {r[1]}: {r[2]}%")
                    st.write(f"*적용일: {latest_date}*")
                else:
                    st.warning("⚠️ 배합비 정보 없음")
            
            with col_right:
                st.markdown("##### ✏️ 수정하기")
                
                # 수정 입력 폼 (자동으로 현재 값 채움)
                new_date = st.date_input(
                    "판매일",
                    value=pd.to_datetime(record['sale_date']).date(),
                    key=f"edit_sale_date_{selected_id}"
                )
                new_date = new_date.strftime('%Y-%m-%d') if new_date else None
                
                new_product = st.text_input(
                    "제품명",
                    value=record['product_name'],
                    key=f"edit_sale_product_{selected_id}"
                )
                
                new_quantity = st.number_input(
                    "수량 (kg)",
                    value=float(record['quantity_kg']),
                    min_value=0.0,
                    step=0.1,
                    key=f"edit_sale_qty_{selected_id}"
                )
                
                new_unit_price = st.number_input(
                    "단가 (원/kg)",
                    value=float(record['unit_price']),
                    min_value=0.0,
                    step=100.0,
                    key=f"edit_sale_price_{selected_id}"
                )
                
                new_customer = st.text_input(
                    "거래처",
                    value=record['customer'] if record['customer'] else "",
                    key=f"edit_sale_customer_{selected_id}"
                )
                
                st.info(f"수정 후 총액: {new_quantity * new_unit_price:,.0f} 원")
                
                # 날짜 변경 시 배합비 확인
                if new_date != pd.to_datetime(record['sale_date']).date():
                    conn = get_db_connection()
                    conn.execute("""
                        SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                        FROM blend_recipes
                        WHERE product_name = ?
                        AND (effective_date IS NULL OR effective_date <= ?)
                        ORDER BY effective_date DESC
                    """, (new_product, new_date))
                    
                    new_recipe_records = conn.execute("""
                        SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                        FROM blend_recipes
                        WHERE product_name = ?
                        AND (effective_date IS NULL OR effective_date <= ?)
                        ORDER BY effective_date DESC
                    """, (new_product, new_date)).fetchall()
                    conn.close()
                    
                    if new_recipe_records:
                        new_latest_date = new_recipe_records[0][3]
                        if latest_date != new_latest_date:
                            st.warning(f"⚠️ 날짜 변경으로 배합비가 달라집니다! ({latest_date} → {new_latest_date})")
                
                # 수정/삭제 버튼
                col_edit, col_delete = st.columns(2)
                
                with col_edit:
                    if st.button("✅ 수정 적용", key="sale_edit_btn", type="primary"):
                        if new_product and new_quantity > 0 and new_unit_price > 0:
                            conn = get_db_connection()
                            # 1. 기존 판매로 차감된 생두 복원
                            conn.execute("""
                                SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                                FROM blend_recipes
                                WHERE product_name = ?
                                AND (effective_date IS NULL OR effective_date <= ?)
                                ORDER BY effective_date DESC
                            """, (record['product_name'], record['sale_date']))
                            
                            old_recipe_records = conn.execute("""
                                SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                                FROM blend_recipes
                                WHERE product_name = ?
                                AND (effective_date IS NULL OR effective_date <= ?)
                                ORDER BY effective_date DESC
                            """, (record['product_name'], record['sale_date'])).fetchall()
                            if old_recipe_records:
                                old_latest_date = old_recipe_records[0][3]
                                old_recipe = [r for r in old_recipe_records if r[3] == old_latest_date]
                                
                                old_green_bean_needed = round(float(record['quantity_kg']) * ROASTING_LOSS_RATE, 3)
                                
                                for origin, product, ratio, _ in old_recipe:
                                    restore_qty = round(old_green_bean_needed * (ratio / 100), 3)
                                    update_green_bean_inventory(origin, product, restore_qty)  # 복원 (양수)
                                    add_inventory_transaction(
                                        new_date, 'sale_edit', 'green_bean',
                                        origin, product, restore_qty, selected_id,
                                        f"판매 수정으로 인한 생두 복원 - {record['product_name']}"
                                    )
                            
                            # 2. 새로운 판매로 생두 차감
                            conn.execute("""
                                SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                                FROM blend_recipes
                                WHERE product_name = ?
                                AND (effective_date IS NULL OR effective_date <= ?)
                                ORDER BY effective_date DESC
                            """, (new_product, new_date))
                            
                            new_recipe_records = conn.execute("""
                                SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                                FROM blend_recipes
                                WHERE product_name = ?
                                AND (effective_date IS NULL OR effective_date <= ?)
                                ORDER BY effective_date DESC
                            """, (new_product, new_date)).fetchall()
                            if new_recipe_records:
                                new_latest_date = new_recipe_records[0][3]
                                new_recipe = [r for r in new_recipe_records if r[3] == new_latest_date]
                                
                                new_green_bean_needed = round(new_quantity * ROASTING_LOSS_RATE, 3)
                                
                                for origin, product, ratio, _ in new_recipe:
                                    deduct_qty = round(new_green_bean_needed * (ratio / 100), 3)
                                    update_green_bean_inventory(origin, product, -deduct_qty)  # 차감 (음수)
                                    add_inventory_transaction(
                                        new_date, 'sale_edit', 'green_bean',
                                        origin, product, -deduct_qty, selected_id,
                                        f"판매 수정 후 생두 차감 - {new_product}"
                                    )
                            
                            # 3. 판매 데이터 업데이트
                            new_total = new_quantity * new_unit_price
                            conn.execute("""
                                UPDATE product_sales
                                SET sale_date=?, product_name=?, quantity_kg=?, 
                                    unit_price=?, total_amount=?, customer=?
                                WHERE id=?
                            """, (new_date, new_product, new_quantity, 
                                  new_unit_price, new_total, new_customer, selected_id))
                            
                            conn.commit()
                            conn.close()
                            
                            st.toast("✅ 수정 완료!", icon="✅")
                            st.success("✅ 수정 완료! (생두 재고 재계산됨)")
                            time.sleep(1)  # 메시지 표시
                            st.rerun()
                        else:
                            st.error("⚠️ 모든 필수 항목을 입력해주세요.")
                
                with col_delete:
                    if st.button("⚠️ 삭제하기", key="sale_delete_btn", type="secondary"):
                        conn = get_db_connection()
                        # 1. 차감된 생두 복원
                        conn.execute("""
                            SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                            FROM blend_recipes
                            WHERE product_name = ?
                            AND (effective_date IS NULL OR effective_date <= ?)
                            ORDER BY effective_date DESC
                        """, (record['product_name'], record['sale_date']))
                        
                        recipe_records = conn.execute("""
                            SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                            FROM blend_recipes
                            WHERE product_name = ?
                            AND (effective_date IS NULL OR effective_date <= ?)
                            ORDER BY effective_date DESC
                        """, (record['product_name'], record['sale_date'])).fetchall()
                        if recipe_records:
                            latest_date = recipe_records[0][3]
                            recipe = [r for r in recipe_records if r[3] == latest_date]
                            
                            green_bean_needed = round(float(record['quantity_kg']) * ROASTING_LOSS_RATE, 3)
                            
                            for origin, product, ratio, _ in recipe:
                                restore_qty = round(green_bean_needed * (ratio / 100), 3)
                                update_green_bean_inventory(origin, product, restore_qty)  # 복원 (양수)
                                add_inventory_transaction(
                                    record['sale_date'], 'sale_delete', 'green_bean',
                                    origin, product, restore_qty, selected_id,
                                    f"판매 삭제로 인한 생두 복원 - {record['product_name']} (환불)"
                                )
                        
                        # 2. 판매 데이터 삭제
                        conn.execute("DELETE FROM product_sales WHERE id=?", (selected_id,))
                        
                        conn.commit()
                        conn.close()
                        
                        # session_state 초기화
                        if 'selected_sale_id' in st.session_state:
                            del st.session_state.selected_sale_id
                        
                        st.toast("✅ 삭제 완료!", icon="✅")
                        st.success("✅ 삭제 완료! (생두 재고 복원됨)")
                        time.sleep(1)  # 메시지 표시
                        st.rerun()
        else:
            st.info("등록된 판매 데이터가 없습니다.")

# ============================================
# 📊 데이터 조회 및 분석 메뉴
# ============================================
elif menu == "📊 데이터 조회 및 분석":
    st.header("📊 데이터 조회 및 분석")
    
    tab1, tab2, tab3 = st.tabs(["생두 매입", "제품 판매", "배합비"])
    
    # 생두 매입 분석
    with tab1:
        st.subheader("🌱 생두 매입 분석")
        
        conn = get_db_connection()
        purchases_df = execute_to_dataframe("""
            SELECT purchase_date, origin, product_name, quantity_kg, unit_price, total_amount, supplier
            FROM green_bean_purchases
            ORDER BY purchase_date
        """)
        conn.close()
        
        if len(purchases_df) > 0:
            purchases_df['purchase_date'] = pd.to_datetime(purchases_df['purchase_date'])
            purchases_df['full_name'] = purchases_df.apply(
                lambda row: get_bean_full_name(row['origin'], row['product_name']), axis=1
            )
            
            st.dataframe(purchases_df)
            
            st.markdown("### 📊 요약 통계")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("총 매입량", f"{purchases_df['quantity_kg'].sum():,.1f} kg")
            with col2:
                st.metric("총 매입금액", f"{purchases_df['total_amount'].sum():,.0f} 원")
            with col3:
                avg_price = purchases_df['total_amount'].sum() / purchases_df['quantity_kg'].sum()
                st.metric("평균 단가", f"{avg_price:,.0f} 원/kg")
        else:
            st.info("등록된 생두 매입 데이터가 없습니다.")
    
    # 제품 판매 분석
    with tab2:
        st.subheader("📦 제품 판매 분석")
        
        conn = get_db_connection()
        sales_df = execute_to_dataframe("""
            SELECT sale_date, product_name, quantity_kg, unit_price, total_amount, customer
            FROM product_sales
            ORDER BY sale_date
        """)
        conn.close()
        
        if len(sales_df) > 0:
            sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
            
            st.dataframe(sales_df)
            
            st.markdown("### 📊 요약 통계")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("총 판매량", f"{sales_df['quantity_kg'].sum():,.1f} kg")
            with col2:
                st.metric("총 매출", f"{sales_df['total_amount'].sum():,.0f} 원")
            with col3:
                st.metric("거래처 수", f"{sales_df['customer'].nunique()}개")
        else:
            st.info("등록된 제품 판매 데이터가 없습니다.")
    
    # 배합비 조회
    with tab3:
        st.subheader("🧪 배합비 조회")
        
        conn = get_db_connection()
        recipes_df = execute_to_dataframe("""
            SELECT product_name, green_bean_origin, green_bean_product, blend_ratio
            FROM blend_recipes
            ORDER BY product_name, blend_ratio DESC
        """)
        conn.close()
        
        if len(recipes_df) > 0:
            recipes_df['full_name'] = recipes_df.apply(
                lambda row: get_bean_full_name(row['green_bean_origin'], row['green_bean_product']), axis=1
            )
            
            st.dataframe(recipes_df[['product_name', 'full_name', 'blend_ratio']])
            
            st.markdown("### 제품별 상세 보기")
            products = recipes_df['product_name'].unique()
            
            for product in products:
                with st.expander(f"📦 {product}"):
                    product_recipe = recipes_df[recipes_df['product_name'] == product]
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.dataframe(product_recipe[['full_name', 'blend_ratio']])
                    with col2:
                        fig = px.pie(product_recipe, values='blend_ratio', names='full_name',
                                   title=f'{product} 배합비')
                        st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("등록된 배합비가 없습니다.")

# ============================================
# 📦 재고 관리 메뉴
# ============================================
elif menu == "📦 재고 관리":
    st.header("📦 재고 관리")
    
    st.info(f"✨ 이 시스템은 판매 시 자동으로 생두를 차감합니다! (원두 1kg = 생두 {ROASTING_LOSS_RATE}kg)")
    
    tab1, tab2 = st.tabs(["재고 현황", "재고 이동 이력"])
    
    # 재고 현황
    with tab1:
        st.subheader("📊 현재 재고 현황")
        
        st.markdown("### 🌱 생두 재고 (가중평균 단가 포함)")
        conn = get_db_connection()
        
        # 가중평균 단가와 함께 재고 조회
        green_inv = execute_to_dataframe("""
            SELECT 
                i.bean_origin,
                i.bean_product,
                i.current_stock_kg,
                i.last_updated,
                COALESCE(
                    (SELECT SUM(p.quantity_kg * p.unit_price) / NULLIF(SUM(p.quantity_kg), 0)
                     FROM green_bean_purchases p
                     WHERE p.origin = i.bean_origin AND p.product_name = i.bean_product
                    ), 0
                ) as weighted_avg_price,
                (SELECT MAX(purchase_date) FROM green_bean_purchases p 
                 WHERE p.origin = i.bean_origin AND p.product_name = i.bean_product) as last_purchase_date,
                (SELECT MIN(purchase_date) FROM green_bean_purchases p 
                 WHERE p.origin = i.bean_origin AND p.product_name = i.bean_product) as first_purchase_date,
                (SELECT COUNT(*) FROM green_bean_purchases p 
                 WHERE p.origin = i.bean_origin AND p.product_name = i.bean_product) as purchase_count
            FROM green_bean_inventory i
            WHERE i.current_stock_kg > 0
            ORDER BY i.current_stock_kg DESC
        """)
        conn.close()
        
        if len(green_inv) > 0:
            # 품종명 생성
            green_inv['full_name'] = green_inv.apply(
                lambda row: get_bean_full_name(row['bean_origin'], row['bean_product']), axis=1
            )
            
            # 총 재고 금액 계산
            green_inv['total_value'] = green_inv['current_stock_kg'] * green_inv['weighted_avg_price']
            
            # 표시용 데이터프레임
            display_df = green_inv[[
                'full_name', 
                'current_stock_kg', 
                'weighted_avg_price', 
                'total_value',
                'purchase_count',
                'first_purchase_date',
                'last_purchase_date'
            ]].copy()
            
            display_df.columns = [
                '생두 품종',
                '현재 재고 (kg)',
                '가중평균 단가 (원/kg)',
                '총 재고 금액 (원)',
                '매입 횟수',
                '최초 입고일',
                '최근 입고일'
            ]
            
            # 테이블 표시
            st.dataframe(
                display_df.style.format({
                    '현재 재고 (kg)': '{:,.1f}',
                    '가중평균 단가 (원/kg)': '{:,.0f}',
                    '총 재고 금액 (원)': '{:,.0f}',
                    '매입 횟수': '{:,.0f}'
                }),
                use_container_width=True
            )
            
            # 요약 정보
            col1, col2, col3 = st.columns(3)
            with col1:
                total_stock = green_inv['current_stock_kg'].sum()
                st.metric("총 생두 재고", f"{total_stock:,.1f} kg")
            with col2:
                total_value = green_inv['total_value'].sum()
                st.metric("총 재고 금액", f"{total_value:,.0f} 원")
            with col3:
                avg_price = total_value / total_stock if total_stock > 0 else 0
                st.metric("전체 평균 단가", f"{avg_price:,.0f} 원/kg")
            
            # 입고 내역 상세
            st.markdown("---")
            st.markdown("#### 📦 입고 내역 상세")
            
            selected_bean = st.selectbox(
                "품종 선택",
                options=green_inv['full_name'].tolist(),
                key="inventory_detail_select"
            )
            
            if selected_bean:
                # 선택된 품종의 정보
                selected_row = green_inv[green_inv['full_name'] == selected_bean].iloc[0]
                origin = selected_row['bean_origin']
                product = selected_row['bean_product']
                
                # 해당 품종의 입고 내역 조회
                purchases = execute_to_dataframe("""
                    SELECT 
                        purchase_date as '입고일',
                        quantity_kg as '수량 (kg)',
                        unit_price as '단가 (원/kg)',
                        total_amount as '총액 (원)',
                        supplier as '공급처'
                    FROM green_bean_purchases
                    WHERE origin = ? AND product_name = ?
                    ORDER BY purchase_date DESC
                """, (origin, product))
                
                if len(purchases) > 0:
                    st.dataframe(
                        purchases.style.format({
                            '수량 (kg)': '{:,.1f}',
                            '단가 (원/kg)': '{:,.0f}',
                            '총액 (원)': '{:,.0f}'
                        }),
                        use_container_width=True
                    )
                    
                    # 단가 추이 차트
                    purchases_chart = execute_to_dataframe("""
                        SELECT purchase_date, unit_price
                        FROM green_bean_purchases
                        WHERE origin = ? AND product_name = ?
                        ORDER BY purchase_date
                    """, (origin, product))
                    
                    if len(purchases_chart) > 0:
                        fig = px.line(
                            purchases_chart, 
                            x='purchase_date', 
                            y='unit_price',
                            title=f'{selected_bean} 단가 추이',
                            labels={'purchase_date': '입고일', 'unit_price': '단가 (원/kg)'}
                        )
                        fig.update_traces(mode='lines+markers')
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("입고 내역이 없습니다.")
            
            # 재고 부족 경고
            low_stock = green_inv[green_inv['current_stock_kg'] < 10]
            if len(low_stock) > 0:
                st.warning(f"⚠️ 재고 부족 (10kg 미만): {', '.join(low_stock['full_name'].tolist())}")
            
            # 재고 차트
            fig = px.bar(green_inv, x='full_name', y='current_stock_kg',
                       title='생두별 현재 재고량',
                       labels={'full_name': '생두', 'current_stock_kg': '재고량 (kg)'})
            st.plotly_chart(fig, use_container_width=True)
            st.info("생두 재고 데이터가 없습니다.")
    
    # 재고 이동 이력
    with tab2:
        st.subheader("📜 재고 이동 이력")
        
        conn = get_db_connection()
        transactions = execute_to_dataframe("""
            SELECT transaction_date, transaction_type, bean_origin, bean_product, 
                   quantity_kg, notes, created_at
            FROM inventory_transactions
            ORDER BY transaction_date DESC, created_at DESC
            LIMIT 100
        """)
        conn.close()
        
        if len(transactions) > 0:
            transactions['full_name'] = transactions.apply(
                lambda row: get_bean_full_name(row['bean_origin'], row['bean_product']), axis=1
            )
            
            st.dataframe(transactions[['transaction_date', 'transaction_type', 'full_name', 'quantity_kg', 'notes']].style.format({
                'quantity_kg': '{:,.1f}'
            }))
        else:
            st.info("재고 이동 이력이 없습니다.")

# ============================================
# 🔬 배합 계산기 메뉴
# ============================================
elif menu == "🔬 배합 계산기":
    st.header("🔬 배합 계산기")
    
    st.info(f"💡 로스팅 손실 {int((ROASTING_LOSS_RATE-1)*100)}% 반영 (원두 1kg = 생두 {ROASTING_LOSS_RATE}kg)")
    
    # 제품 목록 가져오기
    conn = get_db_connection()
    products = execute_to_dataframe("""
        SELECT DISTINCT product_name FROM blend_recipes
        ORDER BY product_name
    """)
    
    if len(products) > 0:
        # 제품 선택
        selected_product = st.selectbox(
            "제품 선택",
            products['product_name'].tolist(),
            help="배합비를 확인할 제품을 선택하세요"
        )
        
        # 생산량 선택
        st.markdown("### 생산량 선택")
        production_amount = st.radio(
            "원두 생산량",
            [1, 15, 20, 50],
            format_func=lambda x: f"{x}kg",
            horizontal=True
        )
        
        # 배합비 조회
        recipe = execute_to_dataframe("""
            SELECT green_bean_origin, green_bean_product, blend_ratio
            FROM blend_recipes
            WHERE product_name = ?
            ORDER BY blend_ratio DESC
        """, [selected_product,])
        
        # 생두 재고 조회
        green_inv = execute_query_to_df(conn, """
            SELECT bean_origin, bean_product, current_stock_kg
            FROM green_bean_inventory
        """)
        conn.close()
        
        if len(recipe) > 0:
            # 생두 필요량 계산
            green_bean_needed = production_amount * ROASTING_LOSS_RATE
            
            # 각 생두별 투입량 계산
            recipe['required_kg'] = (recipe['blend_ratio'] / 100) * green_bean_needed
            recipe['full_name'] = recipe.apply(lambda row: get_bean_full_name(row['green_bean_origin'], row['green_bean_product']), axis=1)
            
            # 재고 정보 병합
            green_inv['full_name'] = green_inv.apply(lambda row: get_bean_full_name(row['bean_origin'], row['bean_product']), axis=1)
            recipe = recipe.merge(green_inv[['full_name', 'current_stock_kg']], on='full_name', how='left')
            recipe['current_stock_kg'] = recipe['current_stock_kg'].fillna(0)
            recipe['stock_sufficient'] = recipe['current_stock_kg'] >= recipe['required_kg']
            
            # 큰 화면으로 표시
            st.markdown("---")
            st.markdown("## 📊 배합 정보")
            
            # 요약 정보 (크게!)
            st.markdown(f"### 🎯 원두 **{production_amount}kg** 생산")
            st.markdown(f"### → 생두 **{green_bean_needed:.1f}kg** 투입 필요")
            
            st.markdown("---")
            
            # 배합 테이블 (보기 좋게!)
            st.markdown("### 📋 생두 투입량")
            
            # 테이블 형식으로 표시
            col_widths = [4, 2, 2, 2]
            
            # 헤더
            cols = st.columns(col_widths)
            cols[0].markdown("**생두 (원산지 - 제품)**")
            cols[1].markdown("**비율**")
            cols[2].markdown("**투입량**")
            cols[3].markdown("**현재 재고**")
            
            st.markdown("---")
            
            # 데이터 행
            for _, row in recipe.iterrows():
                cols = st.columns(col_widths)
                
                # 재고 부족 시 빨간색으로 표시
                if not row['stock_sufficient']:
                    cols[0].markdown(f"**:red[{row['full_name']}]**")
                    cols[1].markdown(f"**:red[{row['blend_ratio']:.1f}%]**")
                    cols[2].markdown(f"**:red[{row['required_kg']:.2f} kg]**")
                    cols[3].markdown(f"**:red[{row['current_stock_kg']:.1f} kg ⚠️]**")
                else:
                    cols[0].markdown(f"**{row['full_name']}**")
                    cols[1].markdown(f"{row['blend_ratio']:.1f}%")
                    cols[2].markdown(f"**{row['required_kg']:.2f} kg**")
                    cols[3].markdown(f"{row['current_stock_kg']:.1f} kg ✅")
            
            st.markdown("---")
            
            # 합계
            cols = st.columns(col_widths)
            cols[0].markdown("**합계**")
            cols[1].markdown("**100%**")
            cols[2].markdown(f"**{recipe['required_kg'].sum():.2f} kg**")
            cols[3].markdown("")
            
            st.markdown("---")
            
            # 재고 상태 요약
            st.markdown("### ✅ 재고 상태")
            
            insufficient = recipe[~recipe['stock_sufficient']]
            if len(insufficient) > 0:
                st.error("⚠️ **재고 부족!**")
                for _, row in insufficient.iterrows():
                    shortage = row['required_kg'] - row['current_stock_kg']
                    st.write(f"- **{row['full_name']}**: {shortage:.1f}kg 부족 (필요: {row['required_kg']:.1f}kg, 현재: {row['current_stock_kg']:.1f}kg)")
            else:
                st.success("✅ **모든 생두 재고 충분!** 생산 가능합니다!")
            
            # 시각화
            st.markdown("---")
            st.markdown("### 📊 배합비 시각화")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # 비율 파이 차트
                fig1 = px.pie(
                    recipe,
                    values='blend_ratio',
                    names='full_name',
                    title=f'{selected_product} 배합비율'
                )
                st.plotly_chart(fig1, use_container_width=True)
            
            with col2:
                # 투입량 바 차트
                fig2 = px.bar(
                    recipe,
                    x='full_name',
                    y='required_kg',
                    title=f'생두별 투입량 ({production_amount}kg 생산 기준)',
                    labels={'full_name': '생두', 'required_kg': '투입량 (kg)'}
                )
                st.plotly_chart(fig2, use_container_width=True)
            
        else:
            st.warning(f"⚠️ {selected_product}의 배합비가 등록되어 있지 않습니다.")
    else:
        st.warning("⚠️ 등록된 배합비가 없습니다. 먼저 '데이터 입력 > 배합비 관리'에서 배합비를 등록해주세요.")

# ============================================
# ✏️ 데이터 수정/삭제 메뉴 (간소화)
# ============================================
elif menu == "✏️ 데이터 수정/삭제":
    st.header("✏️ 데이터 수정 및 삭제")
    st.info("💡 주요 데이터 수정/삭제 기능입니다. 재고 이력은 자동으로 기록됩니다.")
    
    tab1, tab2, tab3 = st.tabs(["생두 매입", "배합비", "판매 데이터"])
    
    # 생두 매입 수정/삭제
    with tab1:
        st.subheader("🌱 생두 매입 수정/삭제")
        
        st.info("💡 수정할 데이터를 선택하면 자동으로 값이 입력됩니다.")
        
        conn = get_db_connection()
        purchases_df = execute_to_dataframe("""
            SELECT id, purchase_date, origin, product_name, quantity_kg, 
                   unit_price, total_amount, supplier
            FROM green_bean_purchases
            ORDER BY purchase_date DESC
            LIMIT 50
        """)
        conn.close()
        
        if len(purchases_df) > 0:
            # 테이블 표시
            st.dataframe(purchases_df, use_container_width=True)
            
            # 🔧 새로운 UI: 드롭다운으로 선택
            purchase_options = [
                f"ID {row['id']} | {row['purchase_date']} | {row['origin']} - {row['product_name']} | {row['quantity_kg']}kg"
                for _, row in purchases_df.iterrows()
            ]
            
            # session_state로 선택 상태 관리
            if 'selected_purchase_id' not in st.session_state:
                st.session_state.selected_purchase_id = purchases_df.iloc[0]['id']
            
            selected_display = st.selectbox(
                "수정/삭제할 데이터 선택",
                purchase_options,
                key="purchase_selector"
            )
            
            # 선택된 ID 추출
            selected_id = int(selected_display.split('|')[0].replace('ID', '').strip())
            
            # 선택이 변경되었는지 감지
            if selected_id != st.session_state.selected_purchase_id:
                st.session_state.selected_purchase_id = selected_id
                time.sleep(1)  # 메시지 표시
                st.rerun()
            
            # 선택된 데이터 가져오기
            record = purchases_df[purchases_df['id'] == selected_id].iloc[0]
            
            # 2컬럼 레이아웃
            col_left, col_right = st.columns([1, 1])
            
            with col_left:
                st.markdown("##### 📋 현재 데이터")
                st.write(f"**ID:** {record['id']}")
                st.write(f"**날짜:** {record['purchase_date']}")
                st.write(f"**원산지:** {record['origin']}")
                st.write(f"**제품명:** {record['product_name']}")
                st.write(f"**수량:** {record['quantity_kg']} kg")
                st.write(f"**단가:** {record['unit_price']:,.0f} 원/kg")
                st.write(f"**총액:** {record['total_amount']:,.0f} 원")
                st.write(f"**공급처:** {record['supplier']}")
            
            with col_right:
                st.markdown("##### ✏️ 수정하기")
                
                # 수정 입력 폼 (자동으로 현재 값 채움)
                new_date = st.date_input(
                    "날짜",
                    value=pd.to_datetime(record['purchase_date']).date(),
                    key=f"edit_purchase_date_{selected_id}"
                )
                new_date = new_date.strftime('%Y-%m-%d') if new_date else None
                
                new_origin = st.text_input(
                    "원산지",
                    value=record['origin'],
                    key=f"edit_purchase_origin_{selected_id}"
                )
                
                new_product = st.text_input(
                    "제품명",
                    value=record['product_name'],
                    key=f"edit_purchase_product_{selected_id}"
                )
                
                new_quantity = st.number_input(
                    "수량 (kg)",
                    value=float(record['quantity_kg']),
                    min_value=0.0,
                    step=0.1,
                    key=f"edit_purchase_qty_{selected_id}"
                )
                
                new_unit_price = st.number_input(
                    "단가 (원/kg)",
                    value=float(record['unit_price']),
                    min_value=0.0,
                    step=100.0,
                    key=f"edit_purchase_price_{selected_id}"
                )
                
                new_supplier = st.text_input(
                    "공급처",
                    value=record['supplier'] if record['supplier'] else "",
                    key=f"edit_purchase_supplier_{selected_id}"
                )
                
                st.info(f"수정 후 총액: {new_quantity * new_unit_price:,.0f} 원")
                
                # 수정/삭제 버튼
                col_edit, col_delete = st.columns(2)
                
                with col_edit:
                    if st.button("✅ 수정 적용", key="purchase_edit_btn", type="primary"):
                        if new_origin and new_product and new_quantity > 0 and new_unit_price > 0:
                            new_total = new_quantity * new_unit_price
                            
                            conn = get_db_connection()
                            conn.execute("""
                                UPDATE green_bean_purchases
                                SET purchase_date=?, origin=?, product_name=?, 
                                    quantity_kg=?, unit_price=?, total_amount=?, supplier=?
                                WHERE id=?
                            """, (new_date, new_origin, new_product, new_quantity, 
                                  new_unit_price, new_total, new_supplier, selected_id))
                            conn.commit()
                            conn.close()
                            
                            st.toast("✅ 수정 완료!", icon="✅")
                            st.success("✅ 수정 완료!")
                            time.sleep(1)  # 메시지 표시
                            st.rerun()
                        else:
                            st.error("⚠️ 모든 필수 항목을 입력해주세요.")
                
                with col_delete:
                    if st.button("⚠️ 삭제하기", key="purchase_delete_btn", type="secondary"):
                        conn = get_db_connection()
                        
                        # 1단계: 삭제할 데이터 조회 (재고 차감용)
                        purchase_data = conn.execute("""
                            SELECT origin, product_name, quantity_kg
                            FROM green_bean_purchases
                            WHERE id = ?
                        """, (selected_id,)).fetchone()
                        
                        if purchase_data:
                            origin, product_name, quantity = purchase_data
                            
                            # 2단계: 재고 차감
                            conn.execute("""
                                UPDATE green_bean_inventory
                                SET current_stock_kg = current_stock_kg - ?,
                                    last_updated = CURRENT_TIMESTAMP
                                WHERE bean_origin = ? AND bean_product = ?
                            """, (quantity, origin, product_name))
                            
                            # 3단계: 재고 이동 이력 기록
                            conn.execute("""
                                INSERT INTO inventory_transactions
                                (transaction_date, transaction_type, item_type, bean_origin, bean_product, 
                                 quantity_kg, reference_id, notes)
                                VALUES (date('now'), 'purchase_delete', 'green_bean', ?, ?, ?, ?, 
                                        '매입 데이터 삭제로 인한 재고 차감')
                            """, (origin, product_name, -quantity, selected_id))
                            
                            # 4단계: 매입 데이터 삭제
                            conn.execute("DELETE FROM green_bean_purchases WHERE id=?", (selected_id,))
                            conn.commit()
                            conn.close()
                            
                            # session_state 초기화
                            if 'selected_purchase_id' in st.session_state:
                                del st.session_state.selected_purchase_id
                            
                            st.toast("✅ 삭제 완료!", icon="✅")
                            st.success("✅ 매입 데이터 삭제 완료!")
                            st.success(f"📦 {product_name} 재고 {quantity}kg 차감")
                            time.sleep(1)
                            st.rerun()
                        else:
                            conn.close()
                            st.error("삭제할 데이터를 찾을 수 없습니다.")
        else:
            st.info("등록된 생두 매입 데이터가 없습니다.")
    
    # 배합비 수정/삭제
    with tab2:
        st.subheader("🧪 배합비 수정/삭제")
        
        st.info("💡 배합비를 수정하거나 삭제할 수 있습니다. 제품을 선택하면 자동으로 현재 배합비가 입력됩니다.")
        
        conn = get_db_connection()
        recipes_df = execute_to_dataframe("""
            SELECT product_name, 
                   GROUP_CONCAT(green_bean_origin || ' - ' || green_bean_product || ' (' || blend_ratio || '%)') as recipe
            FROM blend_recipes
            GROUP BY product_name
        """)
        conn.close()
        
        if len(recipes_df) > 0:
            st.dataframe(recipes_df)
            
            # 🔧 수정: session_state로 제품 변경 감지
            if 'selected_product_for_edit' not in st.session_state:
                st.session_state.selected_product_for_edit = recipes_df['product_name'].tolist()[0]
            
            product_to_edit = st.selectbox(
                "수정/삭제할 제품", 
                recipes_df['product_name'].tolist(),
                key="product_selector"
            )
            
            # 제품이 변경되었는지 감지
            if product_to_edit != st.session_state.selected_product_for_edit:
                st.session_state.selected_product_for_edit = product_to_edit
                # 강제 리렌더링으로 새 값 로드
                time.sleep(1)  # 메시지 표시
                st.rerun()
            
            # 선택한 제품의 현재 배합비 조회
            conn = get_db_connection()
            current_recipe = execute_to_dataframe("""
                SELECT green_bean_origin, green_bean_product, blend_ratio
                FROM blend_recipes
                WHERE product_name = ?
                ORDER BY blend_ratio DESC
            """, [product_to_edit,])
            conn.close()
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("##### 📋 현재 배합비")
                st.dataframe(current_recipe)
            
            with col2:
                st.markdown("##### ✏️ 수정하기")
                st.info(f"💡 {product_to_edit}의 새로운 배합비를 입력하세요 (합계 100%)")
                
                # 적용일자 입력 추가
                new_effective_date = st.date_input(
                    "새 배합비 적용 시작일",
                    date.today(),
                    key=f"edit_effective_date_{product_to_edit}",
                    help="이 날짜부터 새로운 배합비가 적용됩니다"
                )
                new_effective_date = new_effective_date.strftime('%Y-%m-%d') if new_effective_date else None
                
                num_beans = st.number_input("사용할 생두 종류 수", min_value=1, max_value=10, 
                                            value=len(current_recipe), key="edit_num_beans")
                
                new_blend_data = []
                total_ratio = 0
                
                for i in range(num_beans):
                    col_a, col_b, col_c = st.columns(3)
                    with col_a:
                        default_origin = current_recipe.iloc[i]['green_bean_origin'] if i < len(current_recipe) else ""
                        origin = st.text_input(
                            f"원산지 {i+1}", 
                            value=default_origin,
                            key=f"edit_origin_{i}_{product_to_edit}"  # 🔧 제품별로 고유한 키
                        )
                    with col_b:
                        default_product = current_recipe.iloc[i]['green_bean_product'] if i < len(current_recipe) else ""
                        product = st.text_input(
                            f"제품명 {i+1}", 
                            value=default_product,
                            key=f"edit_product_{i}_{product_to_edit}"  # 🔧 제품별로 고유한 키
                        )
                    with col_c:
                        default_ratio = float(current_recipe.iloc[i]['blend_ratio']) if i < len(current_recipe) else 0.0
                        ratio = st.number_input(
                            f"비율 (%)", 
                            min_value=0.0, 
                            max_value=100.0, 
                            value=default_ratio,
                            step=0.1, 
                            key=f"edit_ratio_{i}_{product_to_edit}"  # 🔧 제품별로 고유한 키
                        )
                    
                    if origin and product and ratio > 0:
                        new_blend_data.append((origin, product, ratio))
                        total_ratio += ratio
                
                st.info(f"현재 합계: {total_ratio:.1f}%")
                
                col_edit, col_delete = st.columns(2)
                
                with col_edit:
                    if st.button("✅ 수정 적용", key="blend_edit", type="primary"):
                        if abs(total_ratio - 100) > 0.01:
                            st.error(f"⚠️ 배합비 합계가 100%가 아닙니다. (현재: {total_ratio:.1f}%)")
                        elif len(new_blend_data) == 0:
                            st.error("⚠️ 최소 1개 이상의 생두를 입력해주세요.")
                        else:
                            conn = get_db_connection()
                            # 🔧 수정: 기존 배합비는 삭제하지 않고 새 버전 추가 (이력 관리)
                            
                            # 새 배합비 입력 (적용일자 포함)
                            for origin, product, ratio in new_blend_data:
                                conn.execute("""
                                    INSERT INTO blend_recipes (product_name, effective_date, green_bean_origin, green_bean_product, blend_ratio)
                                    VALUES (?, ?, ?, ?, ?)
                                """, (product_to_edit, new_effective_date, origin, product, ratio))
                            
                            conn.commit()
                            conn.close()
                            st.toast("✅ 수정 완료!", icon="✅")
                            st.success(f"✅ {product_to_edit} 배합비 수정 완료! ({new_effective_date}부터 적용)")
                            time.sleep(1)  # 메시지 표시
                            st.rerun()  # 🔧 수정: 페이지 자동 새로고침
                
                with col_delete:
                    if st.button("⚠️ 배합비 전체 삭제", key="blend_delete", type="secondary"):
                        conn = get_db_connection()
                        conn.execute("DELETE FROM blend_recipes WHERE product_name=?", (product_to_edit,))
                        conn.commit()
                        conn.close()
                        st.toast("✅ 삭제 완료!", icon="✅")
                        st.success(f"✅ {product_to_edit} 배합비 삭제 완료!")
                        # session_state 초기화
                        if 'selected_product_for_edit' in st.session_state:
                            del st.session_state.selected_product_for_edit
                        time.sleep(1)  # 메시지 표시
                        st.rerun()  # 🔧 수정: 페이지 자동 새로고침
        else:
            st.info("등록된 배합비가 없습니다.")
    
    # 판매 데이터 수정/삭제
    with tab3:
        st.subheader("📦 판매 데이터 수정/삭제")
        
        st.info("💡 수정할 판매 데이터를 선택하면 자동으로 값이 입력됩니다.")
        st.warning("⚠️ 삭제 시 차감된 생두 재고가 자동으로 복원됩니다.")
        
        conn = get_db_connection()
        sales_df = execute_query_to_df(conn, """
            SELECT id, sale_date, product_name, quantity_kg, 
                   unit_price, total_amount, customer
            FROM product_sales
            ORDER BY sale_date DESC
            LIMIT 100
        """)
        conn.close()
        
        if len(sales_df) > 0:
            # 테이블 표시
            st.dataframe(sales_df, use_container_width=True)
            
            # 드롭다운 옵션 생성
            sales_options = [
                f"ID {row['id']} | {row['sale_date']} | {row['product_name']} | {row['quantity_kg']}kg | {row['customer']}"
                for _, row in sales_df.iterrows()
            ]
            
            # session_state로 선택 상태 관리
            if 'selected_sale_id' not in st.session_state:
                st.session_state.selected_sale_id = sales_df.iloc[0]['id']
            
            selected_display = st.selectbox(
                "수정/삭제할 판매 데이터 선택",
                sales_options,
                key="sale_selector"
            )
            
            # 선택된 ID 추출
            selected_id = int(selected_display.split('|')[0].replace('ID', '').strip())
            
            # 선택이 변경되었는지 감지
            if selected_id != st.session_state.selected_sale_id:
                st.session_state.selected_sale_id = selected_id
                time.sleep(1)  # 메시지 표시
                st.rerun()
            
            # 선택된 데이터 가져오기
            record = sales_df[sales_df['id'] == selected_id].iloc[0]
            
            # 2컬럼 레이아웃
            col_left, col_right = st.columns([1, 1])
            
            with col_left:
                st.markdown("##### 📋 현재 데이터")
                st.write(f"**ID:** {record['id']}")
                st.write(f"**판매일:** {record['sale_date']}")
                st.write(f"**제품명:** {record['product_name']}")
                st.write(f"**수량:** {record['quantity_kg']} kg")
                st.write(f"**단가:** {record['unit_price']:,.0f} 원/kg")
                st.write(f"**총액:** {record['total_amount']:,.0f} 원")
                st.write(f"**거래처:** {record['customer']}")
                
                # 사용된 배합비 확인
                conn = get_db_connection()
                conn.execute("""
                    SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                    FROM blend_recipes
                    WHERE product_name = ?
                    AND (effective_date IS NULL OR effective_date <= ?)
                    ORDER BY effective_date DESC
                """, (record['product_name'], record['sale_date']))
                
                recipe_records = conn.execute("""
                    SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                    FROM blend_recipes
                    WHERE product_name = ?
                    AND (effective_date IS NULL OR effective_date <= ?)
                    ORDER BY effective_date DESC
                """, (record['product_name'], record['sale_date'])).fetchall()
                conn.close()
                
                if recipe_records:
                    latest_date = recipe_records[0][3]
                    current_recipe = [r for r in recipe_records if r[3] == latest_date]
                    
                    st.markdown("**사용된 배합비:**")
                    for r in current_recipe:
                        st.write(f"- {r[0]} - {r[1]}: {r[2]}%")
                    st.write(f"*적용일: {latest_date}*")
                else:
                    st.warning("⚠️ 배합비 정보 없음")
            
            with col_right:
                st.markdown("##### ✏️ 수정하기")
                
                # 수정 입력 폼 (자동으로 현재 값 채움)
                new_date = st.date_input(
                    "판매일",
                    value=pd.to_datetime(record['sale_date']).date(),
                    key=f"edit_sale_date_{selected_id}"
                )
                new_date = new_date.strftime('%Y-%m-%d') if new_date else None
                
                new_product = st.text_input(
                    "제품명",
                    value=record['product_name'],
                    key=f"edit_sale_product_{selected_id}"
                )
                
                new_quantity = st.number_input(
                    "수량 (kg)",
                    value=float(record['quantity_kg']),
                    min_value=0.0,
                    step=0.1,
                    key=f"edit_sale_qty_{selected_id}"
                )
                
                new_unit_price = st.number_input(
                    "단가 (원/kg)",
                    value=float(record['unit_price']),
                    min_value=0.0,
                    step=100.0,
                    key=f"edit_sale_price_{selected_id}"
                )
                
                new_customer = st.text_input(
                    "거래처",
                    value=record['customer'] if record['customer'] else "",
                    key=f"edit_sale_customer_{selected_id}"
                )
                
                st.info(f"수정 후 총액: {new_quantity * new_unit_price:,.0f} 원")
                
                # 날짜 변경 시 배합비 확인
                if new_date != pd.to_datetime(record['sale_date']).date():
                    conn = get_db_connection()
                    conn.execute("""
                        SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                        FROM blend_recipes
                        WHERE product_name = ?
                        AND (effective_date IS NULL OR effective_date <= ?)
                        ORDER BY effective_date DESC
                    """, (new_product, new_date))
                    
                    new_recipe_records = conn.execute("""
                        SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                        FROM blend_recipes
                        WHERE product_name = ?
                        AND (effective_date IS NULL OR effective_date <= ?)
                        ORDER BY effective_date DESC
                    """, (new_product, new_date)).fetchall()
                    conn.close()
                    
                    if new_recipe_records:
                        new_latest_date = new_recipe_records[0][3]
                        if latest_date != new_latest_date:
                            st.warning(f"⚠️ 날짜 변경으로 배합비가 달라집니다! ({latest_date} → {new_latest_date})")
                
                # 수정/삭제 버튼
                col_edit, col_delete = st.columns(2)
                
                with col_edit:
                    if st.button("✅ 수정 적용", key="sale_edit_btn", type="primary"):
                        if new_product and new_quantity > 0 and new_unit_price > 0:
                            conn = get_db_connection()
                            # 1. 기존 판매로 차감된 생두 복원
                            conn.execute("""
                                SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                                FROM blend_recipes
                                WHERE product_name = ?
                                AND (effective_date IS NULL OR effective_date <= ?)
                                ORDER BY effective_date DESC
                            """, (record['product_name'], record['sale_date']))
                            
                            old_recipe_records = conn.execute("""
                                SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                                FROM blend_recipes
                                WHERE product_name = ?
                                AND (effective_date IS NULL OR effective_date <= ?)
                                ORDER BY effective_date DESC
                            """, (record['product_name'], record['sale_date'])).fetchall()
                            if old_recipe_records:
                                old_latest_date = old_recipe_records[0][3]
                                old_recipe = [r for r in old_recipe_records if r[3] == old_latest_date]
                                
                                old_green_bean_needed = round(float(record['quantity_kg']) * ROASTING_LOSS_RATE, 3)
                                
                                for origin, product, ratio, _ in old_recipe:
                                    restore_qty = round(old_green_bean_needed * (ratio / 100), 3)
                                    update_green_bean_inventory(origin, product, restore_qty)  # 복원 (양수)
                                    add_inventory_transaction(
                                        new_date, 'sale_edit', 'green_bean',
                                        origin, product, restore_qty, selected_id,
                                        f"판매 수정으로 인한 생두 복원 - {record['product_name']}"
                                    )
                            
                            # 2. 새로운 판매로 생두 차감
                            conn.execute("""
                                SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                                FROM blend_recipes
                                WHERE product_name = ?
                                AND (effective_date IS NULL OR effective_date <= ?)
                                ORDER BY effective_date DESC
                            """, (new_product, new_date))
                            
                            new_recipe_records = conn.execute("""
                                SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                                FROM blend_recipes
                                WHERE product_name = ?
                                AND (effective_date IS NULL OR effective_date <= ?)
                                ORDER BY effective_date DESC
                            """, (new_product, new_date)).fetchall()
                            if new_recipe_records:
                                new_latest_date = new_recipe_records[0][3]
                                new_recipe = [r for r in new_recipe_records if r[3] == new_latest_date]
                                
                                new_green_bean_needed = round(new_quantity * ROASTING_LOSS_RATE, 3)
                                
                                for origin, product, ratio, _ in new_recipe:
                                    deduct_qty = round(new_green_bean_needed * (ratio / 100), 3)
                                    update_green_bean_inventory(origin, product, -deduct_qty)  # 차감 (음수)
                                    add_inventory_transaction(
                                        new_date, 'sale_edit', 'green_bean',
                                        origin, product, -deduct_qty, selected_id,
                                        f"판매 수정 후 생두 차감 - {new_product}"
                                    )
                            
                            # 3. 판매 데이터 업데이트
                            new_total = new_quantity * new_unit_price
                            conn.execute("""
                                UPDATE product_sales
                                SET sale_date=?, product_name=?, quantity_kg=?, 
                                    unit_price=?, total_amount=?, customer=?
                                WHERE id=?
                            """, (new_date, new_product, new_quantity, 
                                  new_unit_price, new_total, new_customer, selected_id))
                            
                            conn.commit()
                            conn.close()
                            
                            st.toast("✅ 수정 완료!", icon="✅")
                            st.success("✅ 수정 완료! (생두 재고 재계산됨)")
                            time.sleep(1)  # 메시지 표시
                            st.rerun()
                        else:
                            st.error("⚠️ 모든 필수 항목을 입력해주세요.")
                
                with col_delete:
                    if st.button("⚠️ 삭제하기", key="sale_delete_btn", type="secondary"):
                        conn = get_db_connection()
                        # 1. 차감된 생두 복원
                        conn.execute("""
                            SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                            FROM blend_recipes
                            WHERE product_name = ?
                            AND (effective_date IS NULL OR effective_date <= ?)
                            ORDER BY effective_date DESC
                        """, (record['product_name'], record['sale_date']))
                        
                        recipe_records = conn.execute("""
                            SELECT green_bean_origin, green_bean_product, blend_ratio, effective_date
                            FROM blend_recipes
                            WHERE product_name = ?
                            AND (effective_date IS NULL OR effective_date <= ?)
                            ORDER BY effective_date DESC
                        """, (record['product_name'], record['sale_date'])).fetchall()
                        if recipe_records:
                            latest_date = recipe_records[0][3]
                            recipe = [r for r in recipe_records if r[3] == latest_date]
                            
                            green_bean_needed = round(float(record['quantity_kg']) * ROASTING_LOSS_RATE, 3)
                            
                            for origin, product, ratio, _ in recipe:
                                restore_qty = round(green_bean_needed * (ratio / 100), 3)
                                update_green_bean_inventory(origin, product, restore_qty)  # 복원 (양수)
                                add_inventory_transaction(
                                    record['sale_date'], 'sale_delete', 'green_bean',
                                    origin, product, restore_qty, selected_id,
                                    f"판매 삭제로 인한 생두 복원 - {record['product_name']} (환불)"
                                )
                        
                        # 2. 판매 데이터 삭제
                        conn.execute("DELETE FROM product_sales WHERE id=?", (selected_id,))
                        
                        conn.commit()
                        conn.close()
                        
                        # session_state 초기화
                        if 'selected_sale_id' in st.session_state:
                            del st.session_state.selected_sale_id
                        
                        st.toast("✅ 삭제 완료!", icon="✅")
                        st.success("✅ 삭제 완료! (생두 재고 복원됨)")
                        time.sleep(1)  # 메시지 표시
                        st.rerun()
        else:
            st.info("등록된 판매 데이터가 없습니다.")

# ============================================
# 📊 데이터 조회 및 분석 메뉴
# ============================================
elif menu == "📊 데이터 조회 및 분석":
    st.header("📊 데이터 조회 및 분석")
    
    tab1, tab2, tab3 = st.tabs(["생두 매입", "제품 판매", "배합비"])
    
    # 생두 매입 분석
    with tab1:
        st.subheader("🌱 생두 매입 분석")
        
        conn = get_db_connection()
        purchases_df = execute_to_dataframe("""
            SELECT purchase_date, origin, product_name, quantity_kg, unit_price, total_amount, supplier
            FROM green_bean_purchases
            ORDER BY purchase_date
        """)
        conn.close()
        
        if len(purchases_df) > 0:
            purchases_df['purchase_date'] = pd.to_datetime(purchases_df['purchase_date'])
            purchases_df['full_name'] = purchases_df.apply(
                lambda row: get_bean_full_name(row['origin'], row['product_name']), axis=1
            )
            
            st.dataframe(purchases_df)
            
            st.markdown("### 📊 요약 통계")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("총 매입량", f"{purchases_df['quantity_kg'].sum():,.1f} kg")
            with col2:
                st.metric("총 매입금액", f"{purchases_df['total_amount'].sum():,.0f} 원")
            with col3:
                avg_price = purchases_df['total_amount'].sum() / purchases_df['quantity_kg'].sum()
                st.metric("평균 단가", f"{avg_price:,.0f} 원/kg")
        else:
            st.info("등록된 생두 매입 데이터가 없습니다.")
    
    # 제품 판매 분석
    with tab2:
        st.subheader("📦 제품 판매 분석")
        
        conn = get_db_connection()
        sales_df = execute_to_dataframe("""
            SELECT sale_date, product_name, quantity_kg, unit_price, total_amount, customer
            FROM product_sales
            ORDER BY sale_date
        """)
        conn.close()
        
        if len(sales_df) > 0:
            sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
            
            st.dataframe(sales_df)
            
            st.markdown("### 📊 요약 통계")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("총 판매량", f"{sales_df['quantity_kg'].sum():,.1f} kg")
            with col2:
                st.metric("총 매출", f"{sales_df['total_amount'].sum():,.0f} 원")
            with col3:
                st.metric("거래처 수", f"{sales_df['customer'].nunique()}개")
        else:
            st.info("등록된 제품 판매 데이터가 없습니다.")
    
    # 배합비 조회
    with tab3:
        st.subheader("🧪 배합비 조회")
        
        conn = get_db_connection()
        recipes_df = execute_to_dataframe("""
            SELECT product_name, green_bean_origin, green_bean_product, blend_ratio
            FROM blend_recipes
            ORDER BY product_name, blend_ratio DESC
        """)
        conn.close()
        
        if len(recipes_df) > 0:
            recipes_df['full_name'] = recipes_df.apply(
                lambda row: get_bean_full_name(row['green_bean_origin'], row['green_bean_product']), axis=1
            )
            
            st.dataframe(recipes_df[['product_name', 'full_name', 'blend_ratio']])
            
            st.markdown("### 제품별 상세 보기")
            products = recipes_df['product_name'].unique()
            
            for product in products:
                with st.expander(f"📦 {product}"):
                    product_recipe = recipes_df[recipes_df['product_name'] == product]
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.dataframe(product_recipe[['full_name', 'blend_ratio']])
                    with col2:
                        fig = px.pie(product_recipe, values='blend_ratio', names='full_name',
                                   title=f'{product} 배합비')
                        st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("등록된 배합비가 없습니다.")

# ============================================
# 📦 재고 관리 메뉴
# ============================================
elif menu == "📦 재고 관리":
    st.header("📦 재고 관리")
    
    st.info(f"✨ 이 시스템은 판매 시 자동으로 생두를 차감합니다! (원두 1kg = 생두 {ROASTING_LOSS_RATE}kg)")
    
    tab1, tab2 = st.tabs(["재고 현황", "재고 이동 이력"])
    
    # 재고 현황
    with tab1:
        st.subheader("📊 현재 재고 현황")
        
        st.markdown("### 🌱 생두 재고 (가중평균 단가 포함)")
        conn = get_db_connection()
        
        # 가중평균 단가와 함께 재고 조회
        green_inv = execute_to_dataframe("""
            SELECT 
                i.bean_origin,
                i.bean_product,
                i.current_stock_kg,
                i.last_updated,
                COALESCE(
                    (SELECT SUM(p.quantity_kg * p.unit_price) / NULLIF(SUM(p.quantity_kg), 0)
                     FROM green_bean_purchases p
                     WHERE p.origin = i.bean_origin AND p.product_name = i.bean_product
                    ), 0
                ) as weighted_avg_price,
                (SELECT MAX(purchase_date) FROM green_bean_purchases p 
                 WHERE p.origin = i.bean_origin AND p.product_name = i.bean_product) as last_purchase_date,
                (SELECT MIN(purchase_date) FROM green_bean_purchases p 
                 WHERE p.origin = i.bean_origin AND p.product_name = i.bean_product) as first_purchase_date,
                (SELECT COUNT(*) FROM green_bean_purchases p 
                 WHERE p.origin = i.bean_origin AND p.product_name = i.bean_product) as purchase_count
            FROM green_bean_inventory i
            WHERE i.current_stock_kg > 0
            ORDER BY i.current_stock_kg DESC
        """)
        conn.close()
        
        if len(green_inv) > 0:
            # 품종명 생성
            green_inv['full_name'] = green_inv.apply(
                lambda row: get_bean_full_name(row['bean_origin'], row['bean_product']), axis=1
            )
            
            # 총 재고 금액 계산
            green_inv['total_value'] = green_inv['current_stock_kg'] * green_inv['weighted_avg_price']
            
            # 표시용 데이터프레임
            display_df = green_inv[[
                'full_name', 
                'current_stock_kg', 
                'weighted_avg_price', 
                'total_value',
                'purchase_count',
                'first_purchase_date',
                'last_purchase_date'
            ]].copy()
            
            display_df.columns = [
                '생두 품종',
                '현재 재고 (kg)',
                '가중평균 단가 (원/kg)',
                '총 재고 금액 (원)',
                '매입 횟수',
                '최초 입고일',
                '최근 입고일'
            ]
            
            # 테이블 표시
            st.dataframe(
                display_df.style.format({
                    '현재 재고 (kg)': '{:,.1f}',
                    '가중평균 단가 (원/kg)': '{:,.0f}',
                    '총 재고 금액 (원)': '{:,.0f}',
                    '매입 횟수': '{:,.0f}'
                }),
                use_container_width=True
            )
            
            # 요약 정보
            col1, col2, col3 = st.columns(3)
            with col1:
                total_stock = green_inv['current_stock_kg'].sum()
                st.metric("총 생두 재고", f"{total_stock:,.1f} kg")
            with col2:
                total_value = green_inv['total_value'].sum()
                st.metric("총 재고 금액", f"{total_value:,.0f} 원")
            with col3:
                avg_price = total_value / total_stock if total_stock > 0 else 0
                st.metric("전체 평균 단가", f"{avg_price:,.0f} 원/kg")
            
            # 입고 내역 상세
            st.markdown("---")
            st.markdown("#### 📦 입고 내역 상세")
            
            selected_bean = st.selectbox(
                "품종 선택",
                options=green_inv['full_name'].tolist(),
                key="inventory_detail_select"
            )
            
            if selected_bean:
                # 선택된 품종의 정보
                selected_row = green_inv[green_inv['full_name'] == selected_bean].iloc[0]
                origin = selected_row['bean_origin']
                product = selected_row['bean_product']
                
                # 해당 품종의 입고 내역 조회
                purchases = execute_to_dataframe("""
                    SELECT 
                        purchase_date as '입고일',
                        quantity_kg as '수량 (kg)',
                        unit_price as '단가 (원/kg)',
                        total_amount as '총액 (원)',
                        supplier as '공급처'
                    FROM green_bean_purchases
                    WHERE origin = ? AND product_name = ?
                    ORDER BY purchase_date DESC
                """, (origin, product))
                
                if len(purchases) > 0:
                    st.dataframe(
                        purchases.style.format({
                            '수량 (kg)': '{:,.1f}',
                            '단가 (원/kg)': '{:,.0f}',
                            '총액 (원)': '{:,.0f}'
                        }),
                        use_container_width=True
                    )
                    
                    # 단가 추이 차트
                    purchases_chart = execute_to_dataframe("""
                        SELECT purchase_date, unit_price
                        FROM green_bean_purchases
                        WHERE origin = ? AND product_name = ?
                        ORDER BY purchase_date
                    """, (origin, product))
                    
                    if len(purchases_chart) > 0:
                        fig = px.line(
                            purchases_chart, 
                            x='purchase_date', 
                            y='unit_price',
                            title=f'{selected_bean} 단가 추이',
                            labels={'purchase_date': '입고일', 'unit_price': '단가 (원/kg)'}
                        )
                        fig.update_traces(mode='lines+markers')
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("입고 내역이 없습니다.")
            
            # 재고 부족 경고
            low_stock = green_inv[green_inv['current_stock_kg'] < 10]
            if len(low_stock) > 0:
                st.warning(f"⚠️ 재고 부족 (10kg 미만): {', '.join(low_stock['full_name'].tolist())}")
            
            # 재고 차트
            fig = px.bar(green_inv, x='full_name', y='current_stock_kg',
                       title='생두별 현재 재고량',
                       labels={'full_name': '생두', 'current_stock_kg': '재고량 (kg)'})
            st.plotly_chart(fig, use_container_width=True)
            st.info("생두 재고 데이터가 없습니다.")
    
    # 재고 이동 이력
    with tab2:
        st.subheader("📜 재고 이동 이력")
        
        conn = get_db_connection()
        transactions = execute_to_dataframe("""
            SELECT transaction_date, transaction_type, bean_origin, bean_product, 
                   quantity_kg, notes, created_at
            FROM inventory_transactions
            ORDER BY transaction_date DESC, created_at DESC
            LIMIT 100
        """)
        conn.close()
        
        if len(transactions) > 0:
            transactions['full_name'] = transactions.apply(
                lambda row: get_bean_full_name(row['bean_origin'], row['bean_product']), axis=1
            )
            
            st.dataframe(transactions[['transaction_date', 'transaction_type', 'full_name', 'quantity_kg', 'notes']].style.format({
                'quantity_kg': '{:,.1f}'
            }))
        else:
            st.info("재고 이동 이력이 없습니다.")

# ============================================
# 💰 손익 분석 메뉴
# ============================================
elif menu == "💰 손익 분석":
    st.header("💰 손익 분석")
    
    st.info(f"✨ 자동으로 로스팅 손실 {ROASTING_LOSS_RATE}배를 반영하여 계산합니다!")
    
    tab1, tab2 = st.tabs(["월별 손익계산서", "제품별 손익 분석"])
    
    # 월별 손익계산서
    with tab1:
        st.subheader("📊 월별 손익계산서")
        
        conn = get_db_connection()
        sales_df = execute_to_dataframe("""
            SELECT sale_date FROM product_sales
            ORDER BY sale_date
        """)
        
        if len(sales_df) > 0:
            sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
            
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("시작일", sales_df['sale_date'].min().date(), key="profit_start")
                start_date = start_date.strftime('%Y-%m-%d') if start_date else None
            with col2:
                end_date = st.date_input("종료일", sales_df['sale_date'].max().date(), key="profit_end")
                end_date = end_date.strftime('%Y-%m-%d') if end_date else None
            
            # 매출 데이터
            sales_query = """
                SELECT 
                    strftime('%Y-%m', sale_date) as month,
                    SUM(total_amount) as revenue,
                    SUM(quantity_kg) as sales_qty
                FROM product_sales
                WHERE sale_date BETWEEN ? AND ?
                GROUP BY month
                ORDER BY month
            """
            monthly_sales = execute_query_to_df(conn, sales_query, [start_date, end_date])
            
            # 배합비 기반 생두 원가 계산 (1.2 배율 적용!)
            profit_data = []
            
            for _, row in monthly_sales.iterrows():
                month = row['month']
                revenue = row['revenue']
                sales_qty = row['sales_qty']
                
                # 해당 월의 판매 제품별 생두 원가 계산
                month_sales = execute_query_to_df(conn, """
                    SELECT product_name, SUM(quantity_kg) as qty
                    FROM product_sales
                    WHERE strftime('%Y-%m', sale_date) = ?
                    GROUP BY product_name
                """, [month,])
                
                total_bean_cost = 0
                
                for _, sale in month_sales.iterrows():
                    product = sale['product_name']
                    qty = sale['qty']
                    
                    # 생두 필요량 계산 (1.2배!)
                    green_bean_needed = qty * ROASTING_LOSS_RATE
                    
                    # 배합비 조회
                    recipe = execute_query_to_df(conn, """
                        SELECT green_bean_origin, green_bean_product, blend_ratio
                        FROM blend_recipes
                        WHERE product_name = ?
                    """, [product,])
                    
                    # 각 생두별 원가 계산
                    for _, bean_row in recipe.iterrows():
                        origin = bean_row['green_bean_origin']
                        product_name = bean_row['green_bean_product']
                        ratio = bean_row['blend_ratio'] / 100
                        bean_qty = green_bean_needed * ratio
                        
                        # 해당 월 이전의 가중평균 생두 단가 사용 (정확한 원가 계산!)
                        bean_price_query = """
                            SELECT SUM(total_amount) / SUM(quantity_kg) as weighted_avg_price
                            FROM green_bean_purchases
                            WHERE origin = ? AND product_name = ?
                            AND purchase_date <= ?
                        """
                        bean_price = execute_query_to_df(conn, 
                            bean_price_query, conn, 
                            params=(origin, product_name, f"{month}-31")
                        )['weighted_avg_price'].iloc[0]
                        
                        if pd.notna(bean_price):
                            total_bean_cost += bean_qty * bean_price
                
                # 변동비 조회
                variable_cost_query = """
                    SELECT cost_per_kg
                    FROM variable_costs
                    WHERE effective_month <= ?
                    ORDER BY effective_month DESC
                    LIMIT 1
                """
                var_cost = execute_query_to_df(conn, 
                    variable_cost_query, conn, 
                    params=(f"{month}-01",)
                )
                
                variable_cost_total = 0
                if len(var_cost) > 0:
                    variable_cost_total = sales_qty * var_cost['cost_per_kg'].iloc[0]
                
                # 매출원가 = 생두 원가 + 변동비
                cogs = total_bean_cost + variable_cost_total
                
                # 매출총이익
                gross_profit = revenue - cogs
                gross_margin = (gross_profit / revenue * 100) if revenue > 0 else 0
                
                profit_data.append({
                    'month': month,
                    'revenue': revenue,
                    'bean_cost': total_bean_cost,
                    'variable_cost': variable_cost_total,
                    'cogs': cogs,
                    'gross_profit': gross_profit,
                    'gross_margin': gross_margin
                })
            
            conn.close()
            
            if len(profit_data) > 0:
                profit_df = pd.DataFrame(profit_data)
                
                st.markdown("### 📋 손익계산서")
                display_df = profit_df.copy()
                display_df.columns = ['월', '매출액', '생두원가', '변동비', '매출원가', '매출총이익', '매출총이익률(%)']
                
                st.dataframe(display_df.style.format({
                    '매출액': '{:,.0f}',
                    '생두원가': '{:,.0f}',
                    '변동비': '{:,.0f}',
                    '매출원가': '{:,.0f}',
                    '매출총이익': '{:,.0f}',
                    '매출총이익률(%)': '{:.1f}%'
                }))
                
                # 월별 추이 차트
                st.markdown("### 📈 월별 손익 추이")
                fig = go.Figure()
                fig.add_trace(go.Bar(x=profit_df['month'], y=profit_df['revenue'], name='매출액'))
                fig.add_trace(go.Bar(x=profit_df['month'], y=profit_df['cogs'], name='매출원가'))
                fig.add_trace(go.Scatter(x=profit_df['month'], y=profit_df['gross_profit'], 
                                       name='매출총이익', mode='lines+markers', yaxis='y2'))
                fig.update_layout(
                    yaxis=dict(title='금액 (원)'),
                    yaxis2=dict(title='매출총이익 (원)', overlaying='y', side='right'),
                    barmode='group',
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("선택한 기간에 데이터가 없습니다.")
        else:
            st.info("판매 데이터가 없습니다.")
        
        conn.close()
    
    # 제품별 손익 분석
    with tab2:
        st.subheader("📦 제품별 손익 분석")
        st.info("준비 중입니다. 월별 손익계산서를 먼저 확인해주세요!")

# Footer
st.markdown("---")
st.markdown(f"💚 Yellowknife Coffee Management System v2.0 B - 원산지/제품 분리 관리 | 로스팅 손실 {ROASTING_LOSS_RATE}배 자동 적용")
