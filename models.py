from pydantic import BaseModel


class Employee(BaseModel):
    emp_id: int
    first_name: str
    last_name: str
    salary: int


class UpdateEmployeeModel(BaseModel):
    emp_id: int
    first_name:str = None
    last_name:str = None
    salary:int = None