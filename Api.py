from fastapi import FastAPI, HTTPException, Query, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from DbContext import Database
from auth import create_access_token, verify_token
from datetime import timedelta
from Models.loginModel import LoginRequest
from fastapi.encoders import jsonable_encoder
from collections import defaultdict
from psycopg2.extras import RealDictCursor
import logging

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log',
    filemode='a'
)
logger = logging.getLogger(__name__)

app = FastAPI()
db = Database()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Safe pattern template
def run_query(query, params=None):
    conn = db.get_connection()
    cursor = None
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return rows
    finally:
        if cursor: cursor.close()
        db.put_connection(conn)


@app.get("/users")
def get_users():
    try:
        rows = run_query("SELECT * FROM tb_dim_user")
        return {"data": rows}
    except Exception as e:
        logger.error(f"Error in /users: {e}")
        return {"error": str(e)}

@app.post("/login")
def login(request_data: LoginRequest, request: Request):
    conn = None
    cursor = None
    try:
        conn = db.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("SELECT * FROM tb_dim_user WHERE email = %s", (request_data.email,))
        user = cursor.fetchone()

        if not user or request_data.password != user["password"]:
            raise HTTPException(status_code=401, detail="Invalid email or password")

        token = create_access_token(
            data={"sub": request_data.email},
            expires_delta=timedelta(minutes=60)
        )

        cursor.execute("""
            INSERT INTO tb_dim_user_login_logs
            (userid, email, login_token, status, reason, ip_address, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            user["user_key"], request_data.email, token, "Success",
            "Login successful", request.client.host, request.headers.get("user-agent")
        ))
        conn.commit()

        return {"access_token": token, "user": user, "token_type": "bearer"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in /login: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if cursor: cursor.close()
        if conn: db.put_connection(conn)

@app.get("/verify-token")
def verify_token_endpoint(payload=Depends(verify_token)):
    conn = None
    cursor = None
    try:
        email = payload.get("sub")
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")

        conn = db.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT * FROM tb_dim_user WHERE email = %s", (email,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return {"valid": True, "user": user}
    finally:
        if cursor: cursor.close()
        if conn: db.put_connection(conn)

@app.get("/getCountry")
def get_country():
    try:
        rows = run_query("""
            SELECT country_id, country_name, latitude, longitude, is_active
            FROM geo.tbglcountry
            WHERE is_deleted = '0'
            ORDER BY is_active DESC
        """)
        return {"data": rows}
    except Exception as e:
        logger.error(f"Error in /getCountry: {e}")
        return {"error": str(e)}

@app.get("/getMallByCountryId")
def get_mall_by_country(country_id: int = Query(...)):
    try:
        rows = run_query("""
            SELECT property_id AS propertyId, name AS propertyName, address,
                   grossleasablearea AS GLA, yearopened,
                   latitude AS propertyLatitude, longitude AS propertyLongitude
            FROM geo.tbglproperty
            WHERE country_id = %s AND is_active = '1' AND is_deleted = '0'
        """, (country_id,))
        return {"properties": rows}
    except Exception as e:
        logger.error(f"Error in /getMallByCountryId: {e}")
        return {"error": str(e)}

@app.get("/getState")
def get_state(country_id: int = Query(...)):
    conn = db.get_connection()
    cursor = None
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT province_id AS provinceId, name AS stateName, latitude, longitude, is_active
            FROM geo.tbglprovinces WHERE country_id = %s AND is_deleted = '0'
        """, (country_id,))
        states = cursor.fetchall()

        cursor.execute("""
            SELECT property_id AS propertyId, name AS propertyName, address,
                   grossleasablearea AS GLA, yearopened,
                   latitude AS propertyLatitude, longitude AS propertyLongitude
            FROM geo.tbglproperty WHERE country_id = %s AND is_active = '1' AND is_deleted = '0'
        """, (country_id,))
        properties = cursor.fetchall()

        return {"states": states, "properties": properties}

    except Exception as e:
        logger.error(f"Error in /getState: {e}")
        return {"error": str(e)}
    finally:
        if cursor: cursor.close()
        db.put_connection(conn)

@app.get("/getCity")
def get_city(state_id: int = Query(...)):
    conn = db.get_connection()
    cursor = None
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT city_id AS cityId, name AS cityName, latitude, longitude, is_active
            FROM geo.tbglcity WHERE province_id = %s AND is_deleted = '0'
        """, (state_id,))
        cities = cursor.fetchall()

        cursor.execute("""
            SELECT property_id AS propertyId, name AS propertyName, address,
                   grossleasablearea AS GLA, yearopened,
                   latitude AS propertyLatitude, longitude AS propertyLongitude
            FROM geo.tbglproperty WHERE province_id = %s AND is_active = '1' AND is_deleted = '0'
        """, (state_id,))
        properties = cursor.fetchall()

        return {"city": cities, "properties": properties}

    except Exception as e:
        logger.error(f"Error in /getCity: {e}")
        return {"error": str(e)}
    finally:
        if cursor: cursor.close()
        db.put_connection(conn)

@app.get("/getMallByCityId")
def get_mall_by_city(city_id: int = Query(...)):
    try:
        rows = run_query("""
            SELECT property_id AS propertyId, name AS propertyName, address,
                   grossleasablearea AS GLA, yearopened,
                   latitude AS propertyLatitude, longitude AS propertyLongitude
            FROM geo.tbglproperty WHERE city_id = %s AND is_active = '1' AND is_deleted = '0'
        """, (city_id,))
        return {"properties": rows}
    except Exception as e:
        logger.error(f"Error in /getMallByCityId: {e}")
        return {"error": str(e)}

@app.get("/getStores")
def get_store_by_mallid(mall_id: int = Query(...)):
    conn = db.get_connection()
    cursor = None
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT 
                c.CategoryName,
                sc.SubCategoryName,
                s.StoreName,
                b.BrandName
            FROM geo.tbglstore s
            JOIN geo.tbms_category c ON s.category_ID = c.CategoryID
            JOIN geo.tbms_subcategory sc ON s.sub_category_ID = sc.SubCategoryID
            JOIN geo.tbglbrand b ON s.BrandID = b.BrandID
            WHERE s.mall_id = %s AND s.is_active = '1' AND s.is_deleted = '0'
        """, (mall_id,))
        rows = cursor.fetchall()

        nested_data = defaultdict(lambda: defaultdict(list))
        for i, row in enumerate(rows):
            category = row.get("CategoryName")
            subcategory = row.get("SubCategoryName")
            store_name = row.get("StoreName")
            brand_name = row.get("BrandName")

            if not all([category, subcategory, store_name, brand_name]):
                logger.warning(f"Skipping incomplete row #{i}: {row}")
                continue

            nested_data[category][subcategory].append({
                "StoreName": store_name,
                "BrandName": brand_name
            })

        result = []
        for cat, subcats in nested_data.items():
            result.append({
                "CategoryName": cat,
                "SubCategories": [
                    {"SubCategoryName": subcat, "Stores": stores}
                    for subcat, stores in subcats.items()
                ]
            })

        return {"stores": jsonable_encoder(result)}
    except Exception as e:
        logger.error(f"Error in /getStores: {e}")
        return {"error": str(e)}
    finally:
        if cursor: cursor.close()
        db.put_connection(conn)
