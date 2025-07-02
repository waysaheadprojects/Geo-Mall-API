from fastapi import FastAPI, HTTPException, Query, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from DbContext import Database
from auth import create_access_token, verify_token
from datetime import timedelta
from Models.loginModel import LoginRequest
from fastapi.encoders import jsonable_encoder
from collections import defaultdict
import logging

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log',
    filemode='a'
)
logger = logging.getLogger(__name__)

app = FastAPI()
db = Database()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------- Endpoints ------------

@app.get("/users")
def get_users():
    conn = None
    cursor = None
    try:
        conn = db.get_connection()
        cursor = conn.cursor(cursor_factory=None)
        cursor.execute("SELECT * FROM tb_dim_user")
        rows = cursor.fetchall()
        return {"data": rows}
    except Exception as e:
        logger.error(f"🚨 Error in /users: {str(e)}")
        return {"error": str(e)}
    finally:
        if cursor: cursor.close()
        if conn: db.put_connection(conn)

@app.post("/login")
def login(request_data: LoginRequest, request: Request):
    conn = None
    cursor = None
    try:
        conn = db.get_connection()
        cursor = conn.cursor(cursor_factory=None)

        cursor.execute("SELECT * FROM tb_dim_user WHERE email = %s", (request_data.email,))
        user = cursor.fetchone()

        if not user or request_data.password != user[2]:  # adjust index based on your columns!
            raise HTTPException(status_code=401, detail="Invalid email or password")

        token = create_access_token(
            data={"sub": request_data.email},
            expires_delta=timedelta(minutes=60)
        )

        insert_query = """
            INSERT INTO tb_dim_user_login_logs 
            (userid, email, login_token, status, reason, ip_address, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            user[0], request_data.email, token, "Success", "Login successful",
            request.client.host, request.headers.get("user-agent")
        ))
        conn.commit()

        return {
            "access_token": token,
            "user": dict(zip([desc[0] for desc in cursor.description], user)),
            "token_type": "bearer"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"🚨 Error in /login: {str(e)}")
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
            raise HTTPException(status_code=401, detail="Invalid token payload")

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
    conn = None
    cursor = None
    try:
        conn = db.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT country_id, country_name, latitude, longitude, is_active FROM geo.tbglcountry WHERE is_deleted = '0' ORDER BY is_active DESC")
        rows = cursor.fetchall()
        return {"data": rows}
    except Exception as e:
        logger.error(f"🚨 Error in /getCountry: {str(e)}")
        return {"error": str(e)}
    finally:
        if cursor: cursor.close()
        if conn: db.put_connection(conn)

@app.get("/getMallByCountryId")
def get_mall_by_country(country_id: int = Query(...)):
    conn = None
    cursor = None
    try:
        conn = db.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT 
                property_id AS propertyId, 
                name AS propertyName, 
                address,
                grossleasablearea AS GLA,
                yearopened,
                latitude AS propertyLatitude, 
                longitude AS propertyLongitude
            FROM geo.tbglproperty
            WHERE country_id = %s AND is_active = '1' AND is_deleted = '0'
        """, (country_id,))
        properties = cursor.fetchall()
        return {"properties": properties}
    except Exception as e:
        logger.error(f"🚨 Error in /getMallByCountryId: {str(e)}")
        return {"error": str(e)}
    finally:
        if cursor: cursor.close()
        if conn: db.put_connection(conn)

@app.get("/getState")
def get_state(country_id: int = Query(...)):
    conn = None
    cursor = None
    try:
        conn = db.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT province_id AS provinceId, name AS stateName, latitude, longitude, is_active
            FROM geo.tbglprovinces
            WHERE country_id = %s AND is_deleted = '0'
        """, (country_id,))
        states = cursor.fetchall()

        cursor.execute("""
            SELECT property_id AS propertyId, name AS propertyName, address, grossleasablearea AS GLA,
                   yearopened, latitude AS propertyLatitude, longitude AS propertyLongitude
            FROM geo.tbglproperty
            WHERE country_id = %s AND is_active = '1' AND is_deleted = '0'
        """, (country_id,))
        properties = cursor.fetchall()

        return {"states": states, "properties": properties}
    except Exception as e:
        logger.error(f"🚨 Error in /getState: {str(e)}")
        return {"error": str(e)}
    finally:
        if cursor: cursor.close()
        if conn: db.put_connection(conn)

@app.get("/getCity")
def get_city(state_id: int = Query(...)):
    conn = None
    cursor = None
    try:
        conn = db.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT city_id AS cityId, name AS cityName, latitude, longitude, is_active
            FROM geo.tbglcity
            WHERE province_id = %s AND is_deleted = '0'
        """, (state_id,))
        cities = cursor.fetchall()

        cursor.execute("""
            SELECT property_id AS propertyId, name AS propertyName, address, grossleasablearea AS GLA,
                   yearopened, latitude AS propertyLatitude, longitude AS propertyLongitude
            FROM geo.tbglproperty
            WHERE province_id = %s AND is_active = '1' AND is_deleted = '0'
        """, (state_id,))
        properties = cursor.fetchall()

        return {"city": cities, "properties": properties}
    except Exception as e:
        logger.error(f"🚨 Error in /getCity: {str(e)}")
        return {"error": str(e)}
    finally:
        if cursor: cursor.close()
        if conn: db.put_connection(conn)

@app.get("/getMallByCityId")
def get_mall_by_city(city_id: int = Query(...)):
    conn = None
    cursor = None
    try:
        conn = db.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT property_id AS propertyId, name AS propertyName, address, grossleasablearea AS GLA,
                   yearopened, latitude AS propertyLatitude, longitude AS propertyLongitude
            FROM geo.tbglproperty
            WHERE city_id = %s AND is_active = '1' AND is_deleted = '0'
        """, (city_id,))
        properties = cursor.fetchall()
        return {"properties": properties}
    except Exception as e:
        logger.error(f"🚨 Error in /getMallByCityId: {str(e)}")
        return {"error": str(e)}
    finally:
        if cursor: cursor.close()
        if conn: db.put_connection(conn)

@app.get("/getStores")
def get_store_by_mallid(mall_id: int = Query(...)):
    conn = None
    cursor = None
    try:
        conn = db.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT c.CategoryName, sc.SubCategoryName, s.StoreName, b.BrandName
            FROM geo.tbglstore s
            JOIN geo.tbms_category c ON s.category_ID = c.CategoryID
            JOIN geo.tbms_subcategory sc ON s.sub_category_ID = sc.SubCategoryID
            JOIN geo.tbglbrand b ON s.BrandID = b.BrandID
            WHERE s.mall_id = %s AND s.is_active = '1' AND s.is_deleted = '0'
        """, (mall_id,))
        rows = cursor.fetchall()

        nested_data = defaultdict(lambda: defaultdict(list))
        for row in rows:
            nested_data[row["CategoryName"]][row["SubCategoryName"]].append({
                "StoreName": row["StoreName"],
                "BrandName": row["BrandName"]
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
        logger.error(f"🚨 Error in /getStores: {str(e)}")
        return {"error": str(e)}
    finally:
        if cursor: cursor.close()
        if conn: db.put_connection(conn)
