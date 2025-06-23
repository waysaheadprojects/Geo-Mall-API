from fastapi import FastAPI, HTTPException, Query,Request,Header,Security,Depends
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
        cursor.execute("SELECT country_id , country_name , latitude , longitude FROM  geo.tbglcountry")
        rows = cursor.fetchall()
        return {"data": rows}
    except Exception as e:
        print("🚨 Error in /getCountry endpoint:", str(e))
        return {"error": str(e)}


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
                longitude AS stateLongitude
            FROM geo.tbglprovinces
            WHERE country_id = %s AND is_active = '1' AND is_deleted = '0'
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
def get_state(state_id: int = Query(..., description="State ID to filter states and properties")):
    try:
        cursor = db.get_cursor()

        # Query for states
        query_city = """
            SELECT 
                city_id AS cityId, 
                name AS cityName, 
                latitude , 
                longitude
            FROM geo.tbglcity
            WHERE province_id = %s AND is_active = '1' AND is_deleted = '0'
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

   
