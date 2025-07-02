from fastapi import FastAPI, HTTPException, Query,Request,Header,Security,DependsMore 
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from DbContext import Database
from auth import create_access_token,verify_token  
from datetime import timedelta
import pdb; 
from Models.loginModel import LoginRequest 
from jose import JWTError
import logging
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from collections import defaultdict
from fastapi.encoders import jsonable_encoder
import traceback
from psycopg2.extras import RealDictCursor


import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Use DEBUG for more detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log',  # Optional: logs to file
    filemode='a'
)


# pdb.set_trace()

logger = logging.getLogger(__name__)

app = FastAPI()
db = Database()

# 🚨 Allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ← This allows requests from any origin
    allow_credentials=True,
    allow_methods=["*"],  # ← Allow all HTTP methods (GET, POST, PUT, etc.)
    allow_headers=["*"],  # ← Allow all headers
)

@app.get("/users")
def get_users():
    try:
        cursor = db.get_cursor()
        cursor.execute("SELECT * FROM tb_dim_user")
        rows = cursor.fetchall()
        return {"data": rows}
    except Exception as e:
        print("🚨 Error in /users endpoint:", str(e))
        return {"error": str(e)}


@app.post("/login")
def login(request_data: LoginRequest, request: Request):
    try:
        cursor = db.get_cursor()
        query = "SELECT * FROM tb_dim_user WHERE email = %s"
        cursor.execute(query, (request_data.email,))
        user = cursor.fetchone()

        login_status = "Failed"
        login_reason = ""
        login_token = None

        if not user:
            login_reason = "User not found"
            raise HTTPException(status_code=401, detail="Invalid email or password")

        if request_data.password != user["password"]:
            login_reason = "Invalid password"
            raise HTTPException(status_code=401, detail="Invalid email or password")

        # Create JWT token
        login_token = create_access_token(
            data={"sub": request_data.email},
            expires_delta=timedelta(minutes=60)
        )
        login_status = "Success"
        login_reason = "Login successful"

        # Insert login log
        insert_log_query = """
            INSERT INTO tb_dim_user_login_logs (userid, email, login_token, status, reason, ip_address, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_log_query, (
            user["user_key"], request_data.email, login_token, login_status, login_reason,
            request.client.host, request.headers.get("user-agent")
        ))
        db.connection.commit()

        return {
            "access_token": login_token,
            "user": user,
            "token_type": "bearer"
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        print("🚨 Error in /login:", str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/verify-token")
def verify_token_endpoint(payload=Depends(verify_token)):
    email = payload.get("sub")
    if not email:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    cursor = db.get_cursor()
    cursor.execute("SELECT * FROM tb_dim_user WHERE email = %s", (email,))
    user = cursor.fetchone()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {"valid": True, "user": user}


@app.get("/getCountry")
def get_country():
    try:
        cursor = db.get_cursor()
        cursor.execute("SELECT country_id , country_name , latitude , longitude , is_active FROM  geo.tbglcountry where is_deleted = '0'")
        cursor.execute("SELECT country_id , country_name , latitude , longitude , is_active FROM  geo.tbglcountry where is_deleted = '0' ORDER BY is_active DESC")
        rows = cursor.fetchall()
        return {"data": rows}
    except Exception as e:
        print("🚨 Error in /getCountry endpoint:", str(e))
        return {"error": str(e)}


@app.get("/getMallByCountryId")
def get_mall_by_country(country_id: int = Query(..., description="Country ID to filter  properties")):
    try:
        cursor = db.get_cursor()

        # Query for properties
        query_properties = """
            SELECT 
                property_id AS propertyId, 
                name AS propertyName, 
                address,
                grossleasablearea as GLA,
                yearopened,
                latitude AS propertyLatitude, 
                longitude AS propertyLongitude
            FROM geo.tbglproperty
            WHERE country_id = %s AND is_active = '1' AND is_deleted = '0'
        """
        cursor.execute(query_properties, (country_id,))
        properties = cursor.fetchall()

        return {
            "properties": properties
        }

    except Exception as e:
        print("🚨 Error in /getMallByCountryId endpoint:", str(e))
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/getState")
def get_state(country_id: int = Query(..., description="Country ID to filter states and properties")):
    try:
        cursor = db.get_cursor()

        # Query for states
        query_states = """
            SELECT 
                province_id AS provinceId, 
                name AS stateName, 
                latitude AS stateLatitude, 
                longitude AS stateLongitude,
                is_active
            FROM geo.tbglprovinces
            WHERE country_id = %s AND is_deleted = '0'
        """
        cursor.execute(query_states, (country_id,))
        states = cursor.fetchall()

        # Query for properties
        query_properties = """
            SELECT 
                property_id AS propertyId, 
                name AS propertyName, 
                address,
                grossleasablearea as GLA,
                yearopened,
                latitude AS propertyLatitude, 
                longitude AS propertyLongitude
            FROM geo.tbglproperty
            WHERE country_id = %s AND is_active = '1' AND is_deleted = '0'
        """
        cursor.execute(query_properties, (country_id,))
        properties = cursor.fetchall()

        return {
            "states": states,
            "properties": properties
        }

    except Exception as e:
        print("🚨 Error in /getState endpoint:", str(e))
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/getCity")
def get_city(state_id: int = Query(..., description="State ID to filter states and properties")):
    try:
        cursor = db.get_cursor()

        # Query for states
        query_city = """
            SELECT 
                city_id AS cityId, 
                name AS cityName, 
                latitude , 
                longitude,
                is_active
            FROM geo.tbglcity
            WHERE province_id = %s AND is_deleted = '0'
        """
        cursor.execute(query_city, (state_id,))
        city = cursor.fetchall()

        # Query for properties
        query_properties = """
            SELECT 
                property_id AS propertyId, 
                name AS propertyName, 
                address,
                grossleasablearea as GLA,
                yearopened,
                latitude AS propertyLatitude, 
                longitude AS propertyLongitude
            FROM geo.tbglproperty
            WHERE province_id = %s AND is_active = '1' AND is_deleted = '0'
        """
        cursor.execute(query_properties, (state_id,))
        properties = cursor.fetchall()

        return {
            "city": city,
            "properties": properties
        }

    except Exception as e:
        print("🚨 Error in /getCity endpoint:", str(e))
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/getMallByCityId")
def get_mall(city_id: int = Query(..., description="City ID to filter states and properties")):
    try:
        cursor = db.get_cursor()

        # Query for properties
        query_properties = """
            SELECT 
                property_id AS propertyId, 
                name AS propertyName, 
                address,
                grossleasablearea as GLA,
                yearopened,
                latitude AS propertyLatitude, 
                longitude AS propertyLongitude
            FROM geo.tbglproperty
            WHERE city_id = %s AND is_active = '1' AND is_deleted = '0'
        """
        cursor.execute(query_properties, (city_id,))
        properties = cursor.fetchall()

        return {
            "properties": properties
        }

    except Exception as e:
        print("🚨 Error in /getMallByCityId endpoint:", str(e))
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/getStores")
def get_store_by_mallid(mall_id: int = Query(..., description="Get store data by mall ID")):
    try:
        logger.info(f"Fetching store data for mall_id={mall_id}")
        cursor = db.get_cursor()

        query = """
            SELECT 
                c.CategoryName,
                sc.SubCategoryName,
                s.StoreName,
                b.BrandName
            FROM geo.tbglstore s
            INNER JOIN geo.tbms_category c ON s.category_ID = c.CategoryID
            INNER JOIN geo.tbms_subcategory sc ON s.sub_category_ID = sc.SubCategoryID
            INNER JOIN geo.tbglbrand b ON s.BrandID = b.BrandID
            WHERE s.mall_id = %s AND s.is_active = '1' AND s.is_deleted = '0'
        """
        cursor.execute(query, (mall_id,))
        rows = cursor.fetchall()
        logger.info(f"Rows fetched: {len(rows)}")

        nested_data = defaultdict(lambda: defaultdict(list))

        for i, row in enumerate(rows):
            try:
                category = row['categoryname']
                subcategory = row['subcategoryname']
                store = {
                    "StoreName": row['storename'],
                    "BrandName": row['brandname']
                }
                nested_data[category][subcategory].append(store)
            except Exception as row_error:
                logger.error(f"Error processing row {i}: {row} -> {str(row_error)}")

        result = []
        for category, subcats in nested_data.items():
            subcategory_list = []
            for subcat, stores in subcats.items():
                subcategory_list.append({
                    "SubCategoryName": subcat,
                    "Stores": stores
                })
            result.append({
                "CategoryName": category,
                "SubCategories": subcategory_list
            })

        return {"stores": jsonable_encoder(result)}

    except Exception as e:
        logger.error(f"Exception in /getStores endpoint: {repr(e)}")
        return JSONResponse(status_code=500, content={"error": "Internal server error"})

    finally:
        try:
            if cursor:
                cursor.close()
        except:
            pass
