
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from database_connection import get_connection
from datetime import datetime
from dateutil import parser

app = FastAPI()

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.get('/get_latest_data_only')
def get_hourly_data():
    try:
        connection = get_connection()
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
        '''
        select max(date) as date from test.loademployee 
        ''')
        last_time = cursor.fetchone()['date']
        cursor.execute('''
            select * from test.newemployees
            where updated_at > %s
        ''',(last_time,))
        employees = cursor.fetchall()
        cursor.close()
        connection.close()
        return {
            'success': True,
            'message':'fetch successfully',
            'data':employees,
            'count':len(employees)
        }
    except Exception as e:
        return {
            'success': False,
            'message':'something went wrong in code execution'
        }

@app.post('/load_latest_data_only')
async def load_hourly_data(request: Request):
    employees = await request.json()
    connection = get_connection()
    cursor = connection.cursor()
    duplicate = []
    for emp in employees:
        try:
            cursor.execute('''
                INSERT INTO test.loademployee
                VALUES (%s, %s, %s, %s, %s)
            ''', (emp['emp_id'], emp['first_name'], emp['last_name'],
                  emp['salary'], emp['updated_at']))
        except Exception as e:
            duplicate.append(emp)

    for emp in duplicate:
        cursor.execute('''
            UPDATE test.loademployee
            SET first_name = %s, last_name = %s, salary = %s, date = %s
            WHERE emp_id = %s
        ''', (emp['first_name'], emp['last_name'], emp['salary'],
              emp['updated_at'], emp['emp_id']))

    connection.commit()
    cursor.close()
    connection.close()

    return {
        'message': 'load successfully',
        'count': len(employees),
        'inserted': len(employees) - len(duplicate),
        'updated': len(duplicate)
    }


@app.get('/get_all_employees_for_source')
def get_data():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute('SELECT * FROM test.newemployees')
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        'success': True,
        'message': 'fetch all employees',
        'data': rows,
        'count': len(rows),
        }

@app.get('/get_all_employees_for_target')
def get_data():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute('SELECT * FROM test.loademployee')
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        'success': True,
        'message': 'fetch all employees',
        'data': rows,
        'count': len(rows),
    }


@app.get('/employees_by_filter')
def get_data(fromdate: str = None, todate: str = None, time: str = None):
    print(fromdate, todate, time)

    fdate = None
    tdate = None
    time_str = None

    if fromdate:
        try:
            fdate = datetime.strptime(fromdate, "%Y-%m-%d").date()
        except Exception:
            return {"success": False, "message": "Invalid 'fromdate' format. Expected YYYY-MM-DD"}

    if todate:
        try:
            tdate = datetime.strptime(todate, "%Y-%m-%d").date()
        except Exception:
            return {"success": False, "message": "Invalid 'todate' format. Expected YYYY-MM-DD"}

    if time:
        try:
            if len(time.split(":")) == 2:
                time_obj = datetime.strptime(time, "%H:%M").time()
            else:
                time_obj = datetime.strptime(time, "%H:%M:%S").time()
            time_str = time_obj.strftime("%H:%M:%S")
        except Exception:
            return {"success": False, "message": "Invalid 'time' format. Expected HH:MM or HH:MM:SS"}

    query1 = '''
        SELECT * FROM test.newemployees
    '''
    query2 = '''
        SELECT * FROM test.newemployees
        WHERE DATE(updated_at) = %s
    '''
    query3 = '''
        SELECT * FROM test.newemployees
        WHERE DATE(updated_at) >= %s AND DATE(updated_at) <= %s
    '''
    query4 = '''
        SELECT * FROM test.newemployees
        WHERE DATE(updated_at) >= %s AND DATE(updated_at) <= %s
        AND TIME(updated_at) = %s
    '''

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # If only fromdate is provided (without todate), query for that specific date
        if fdate and not todate:
            if time_str:
                # Single date with time filter
                cursor.execute(query4, (fdate, fdate, time_str))
            else:
                # Single date without time
                cursor.execute(query2, (fdate,))
        # If both dates are provided, query for date range
        elif fdate and tdate:
            if time_str:
                cursor.execute(query4, (fdate, tdate, time_str))
            else:
                cursor.execute(query3, (fdate, tdate))
        # If no dates provided, return all data
        else:
            cursor.execute(query1)

        rows = cursor.fetchall()

        return {
            "success": True,
            "message": "Fetched data successfully",
            "count": len(rows),
            "data": rows,
        }

    except Exception as e:
        return {"success": False, "message": f"Database error: {str(e)}"}

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.post('/create_employee_for_another_database')
async def create_employee_for_another_database(request: Request):
    emp = await request.json()
    conn = get_connection()
    cursor = conn.cursor()
    print('run fine')
    count = 0
    for employee in emp:
        cursor.execute("""
        INSERT INTO test.loademployee (emp_id, first_name, last_name, salary, date)
        VALUES (%s, %s, %s, %s, %s)
         """, (employee['emp_id'], employee['first_name'], employee['last_name'], employee['salary'], employee['updated_at']))
        count += 1
    conn.commit()
    cursor.close()
    conn.close()
    return JSONResponse(
        status_code=201,
        content={
            'success': True,
            'message': 'Employees created successfully',
            'count': count,
        }
    )


@app.post('/create_employees_in_source')
async def create_employee(request: Request):
    emp = await request.json()
    conn = get_connection()
    cursor = conn.cursor()
    print('run fine')
    for employee in emp:
        cursor.execute("""
        INSERT INTO test.newemployees (emp_id, first_name, last_name, salary)
        VALUES (%s, %s, %s, %s)
         """, (employee['emp_id'], employee['first_name'], employee['last_name'], employee['salary']))
    conn.commit()
    cursor.close()
    conn.close()
    return JSONResponse(
        status_code=201,
        content={
            'success': True,
            'message': 'Employees created successfully',
            'count': len(emp),
        }
    )

@app.patch('/update_employees_in_source')
async def put_data(request: Request):
    emp_ids = []
    employees = await request.json()
    print(employees)

    for emp in employees:
        fields = []
        values = []

        # Use dictionary access with .get() to check if key exists and has a value
        if emp.get('first_name'):
            fields.append('first_name = %s')
            values.append(emp['first_name'])
        if emp.get('last_name'):
            fields.append('last_name = %s')
            values.append(emp['last_name'])
        if emp.get('salary'):
            fields.append('salary = %s')
            values.append(emp['salary'])

        if not fields:
            return JSONResponse(
                content={"success": False, "message": "No fields to update"}
            )

        values.append(emp['emp_id'])

        query = f'''
            update test.newemployees 
            set {', '.join(fields)} 
            where emp_id = %s
        '''

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query, tuple(values))
        conn.commit()
        cursor.close()
        conn.close()
        emp_ids.append(emp['emp_id'])

    return JSONResponse(
        status_code=200,
        content={
            'success': True,
            'message': 'update data successfully',
            'data': emp_ids,
        }
    )





@app.delete("/delete_employee/")
def delete_employee(employee_id:int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM test.employees
        WHERE emp_id = %s
    """, (employee_id,))
    conn.commit()
    cursor.close()
    conn.close()
    return JSONResponse(
        status_code=200,
        content={
            "success": True,
            "message": "Employee deleted successfully",
            "emp_id": employee_id
        }
    )


@app.post('/csv_data/')
async def csv_data(request:Request):
    data = await request.json()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
    create table if not exists test.loadcsvdata (
    sale_id int primary key,
    product varchar(50),
    quantity int,
    price int,
    sale_date date
    )
    ''')
    print("fine")
    for record in data:
        date_time = parser.parse(record['date'])
        date = date_time.strftime("%Y-%m-%d")
        cursor.execute('''
        insert into test.loadcsvdata
        values (%s,%s,%s,%s,%s)
        ''',(record['sale_id'], record['product'], record['quantity'], record['price'], date))
    conn.commit()
    cursor.close()
    conn.close()
    return JSONResponse(
        status_code=200,
        content={
            "success": True,
            "message": "Data inserted successfully",
            "data": data,
            "count": len(data)
        }
    )
