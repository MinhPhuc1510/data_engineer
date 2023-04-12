
import json
import logging

import pandas as pd
import redshift_connector
from backend.forms.employees import DepartmentForm, EmployeeForm, CreateDepartmentForm
from backend.utils.redshif import Redshift
from django.contrib import messages
from django.http import HttpResponseRedirect
from django.shortcuts import render
from django.urls import reverse
from rest_framework import status
from rest_framework.decorators import permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response
from rest_framework.views import APIView
import datetime

_logger = logging.getLogger(__name__)
# Create your views here.
connect = Redshift().conn

class EmployeeView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'employees.html'
    permission_classes = [IsAuthenticated]
    
    def get(self, request):
        try:
            connect = Redshift().conn
            _logger.info('Start Get employees')
            # Create a Cursor object
            _logger.info('Connect to Redshif')
            cursor = connect.cursor()

            # Query a table using the Cursor
            cursor.execute("SELECT p.business_entity_id,p.first_name,p.last_name,e.birth_date,e.salaried_flag,e.vacation_hours,e.sick_leave_hours \
                            FROM adventureworks2008r2_person.person p \
                            INNER JOIN adventureworks2008r2_humanresources.employee e ON p.business_entity_id=e.business_entity_id;")              
            #Retrieve the query result set
            
            query = cursor.fetchall()
            connect.commit()

            if len(query) == 0:
                respones = {
                'data': None
            }
                return Response(data=respones, status=status.HTTP_404_NOT_FOUND)

            df = pd.DataFrame(query)
            df.columns = ['business_entity_id', 'first_name', 'last_name', 'birth_date', 'salaried_flag', 'vacation_hours', 'sick_leave_hours']
            df['birth_date'] = pd.to_datetime(df['birth_date']).astype(str)
            result = df.to_json(orient="records")
            respones = {
                'data': json.loads(result)
            }
            _logger.info('End Get employees')

            return Response(data=respones, status=status.HTTP_200_OK)

        except Exception:
            return HttpResponseRedirect(reverse('user_logout'))



class DetailEmployeeView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'get_detail_employee.html'
    permission_classes = [IsAuthenticated]
    
    def get(self, request, id):
        try:
            connect = Redshift().conn
            _logger.info('Start Get detail employee')
            # Create a Cursor object
            _logger.info('Connect to Redshif')
            cursor = connect.cursor()
        
            cursor.execute(f"SELECT p.business_entity_id,p.first_name,p.last_name,e.birth_date,e.salaried_flag,e.vacation_hours,e.sick_leave_hours,e.hire_date,e.gender,e.job_title,dt.name,d.start_date,d.end_date \
                            FROM adventureworks2008r2_person.person p \
                            INNER JOIN adventureworks2008r2_humanresources.employee e ON p.business_entity_id=e.business_entity_id \
                            INNER JOIN adventureworks2008r2_humanresources.employee_department_history d ON p.business_entity_id=d.business_entity_id \
                            INNER JOIN adventureworks2008r2_humanresources.department dt ON d.department_id=dt.department_id    \
                            WHERE p.business_entity_id={id};")
            
            query = cursor.fetchall()
            if len(query) == 0:
                respones = {
                'data': None
            }
                return Response(data=respones, status=status.HTTP_404_NOT_FOUND)
            df = pd.DataFrame(query)
            df.columns = ['business_entity_id', 'first_name', 'last_name', 'birth_date', 'salaried_flag', 'vacation_hours', 'sick_leave_hours', 'hire_date', 'gender', 'job_title', 'name', 'start_date', 'end_date']
            df['birth_date'] = pd.to_datetime(df['birth_date']).astype(str)
            df['hire_date'] = pd.to_datetime(df['hire_date']).astype(str)
            df['start_date'] = pd.to_datetime(df['start_date']).astype(str)
            df['end_date'] = pd.to_datetime(df['end_date']).astype(str)

            cursor.execute(f"SELECT rate FROM adventureworks2008r2_humanresources.employee_pay_history \
                            WHERE modified_date=(SELECT max(modified_date) FROM adventureworks2008r2_humanresources.employee_pay_history \
                            WHERE business_entity_id={id}) AND business_entity_id={id};")
            result = json.loads(df.to_json(orient="records"))
            rate = cursor.fetchall()
            connect.commit()
            if len(rate) !=0 :
                query = rate[0][0]
                result[0] =  result[0] | {"rate":query}

            respones = {
                'data': result
            }
            _logger.info('End Get detail employee')

            return Response(data=respones, status=status.HTTP_200_OK)

        except Exception:
            return HttpResponseRedirect(reverse('user_logout'))

@permission_classes([IsAuthenticated])
def alert_delete(request, id):
  contex = {'id': id}
  return render(request, 'alert_delete.html', contex)

@permission_classes([IsAuthenticated])
def delete_employee(request, id):
    try:
        connect = Redshift().conn
        _logger.info('Start Delete detail employee')
        # Create a Cursor object
        _logger.info('Connect to Redshif')
        cursor = connect.cursor()
        
        try:
            cursor.execute(f"DELETE FROM adventureworks2008r2_humanresources.employee \
            WHERE business_entity_id={id};")
            connect.commit()         
        except redshift_connector.error.ProgrammingError :
            messages.info(request, 'Failed!')
            return HttpResponseRedirect(reverse('employees'))
        messages.info(request, 'Delete successfully!')
        return HttpResponseRedirect(reverse('employees'))
    except Exception:
            return HttpResponseRedirect(reverse('user_logout'))

@permission_classes([IsAuthenticated])
def update_employee(request, id):
    try:
        connect = Redshift().conn
    # if this is a POST request we need to process the form data
        if request.method == 'POST':
            # create a form instance and populate it with data from the request:
            form = EmployeeForm(request.POST)
            # check whether it's valid:
            if form.is_valid():
                cursor = connect.cursor()
                value = ','.join(f"{key}='{form.cleaned_data[key]}'" for key in form.cleaned_data if key in ['birth_date', 'hire_date', 'gender', 'job_title'])
                query = f"UPDATE adventureworks2008r2_humanresources.employee \
                        SET {value} \
                        WHERE business_entity_id={id};"
                cursor.execute(query)
                

                value_2 = ','.join(f"{key}='{form.cleaned_data[key]}'" for key in form.cleaned_data if key in ['first_name', 'last_name'])
                query_2 = f"UPDATE adventureworks2008r2_person.person \
                        SET {value_2} \
                        WHERE business_entity_id={id};"
                cursor.execute(query_2)

                value_3 = ','.join(f"{key}='{form.cleaned_data[key]}'" for key in form.cleaned_data if key in ['salary'])
                value_3 = value_3.replace('salary', 'rate')
                query_3 = f"UPDATE adventureworks2008r2_humanresources.employee_pay_history \
                        SET {value_3} \
                        WHERE business_entity_id={id};"
                cursor.execute(query_3)
                connect.commit()

                messages.info(request, 'Update sucessfully')
                return HttpResponseRedirect(reverse('detai_employee',args=(id,)))
        
        # if a GET (or any other method) we'll create a blank form
        elif request.method == 'GET':
            cursor = connect.cursor()

            # Query a table using the Cursor
            cursor.execute(f"SELECT p.business_entity_id,p.first_name,p.last_name,e.birth_date,e.salaried_flag,e.vacation_hours,e.sick_leave_hours,e.hire_date,e.gender,e.job_title,dt.name,d.start_date,d.end_date \
                            FROM adventureworks2008r2_person.person p \
                            INNER JOIN adventureworks2008r2_humanresources.employee e ON p.business_entity_id=e.business_entity_id \
                            INNER JOIN adventureworks2008r2_humanresources.employee_department_history d ON p.business_entity_id=d.business_entity_id \
                            INNER JOIN adventureworks2008r2_humanresources.department dt ON d.department_id=dt.department_id    \
                            WHERE p.business_entity_id={id};")
            
            query = cursor.fetchall()
            connect.commit()
            if len(query) == 0:
                respones = {
                'data': None
            }
                return Response(data=respones, status=status.HTTP_404_NOT_FOUND)
            df = pd.DataFrame(query)
            df.columns = ['business_entity_id', 'first_name', 'last_name', 'birth_date', 'salaried_flag', 'vacation_hours', 'sick_leave_hours', 'hire_date', 'gender', 'job_title', 'name', 'start_date', 'end_date']
            df['birth_date'] = pd.to_datetime(df['birth_date']).astype(str)
            df['hire_date'] = pd.to_datetime(df['hire_date']).astype(str)
            df['start_date'] = pd.to_datetime(df['start_date']).astype(str)
            df['end_date'] = pd.to_datetime(df['end_date']).astype(str)

            result = json.loads(df.to_json(orient="records"))[0]    
            form_data = {
                "first_name": result['first_name'],
                "last_name": result['last_name'],
                "birth_date": result['birth_date'],
                "gender": result['gender'],
                "hire_date": result['hire_date'],
                "job_title:": result['job_title']
            }
            
            form_data = EmployeeForm(form_data)

        return render(request, 'employee_form.html', {'form': form_data, 'id':id})
    except Exception:
        return HttpResponseRedirect(reverse('user_logout'))

class DepartmentView(APIView):
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'departments.html'
    permission_classes = [IsAuthenticated]
    def get(self, request):
        try:
            connect = Redshift().conn
            _logger.info('Start Get deparment')
            self.http_method_names.append("GET")
            # Create a Cursor object
            _logger.info('Connect to Redshif')
            cursor = connect.cursor()

            # Query a table using the Cursor
            cursor.execute("SELECT p.department_id,p.name,p.group_name,COUNT(e.department_id) employess \
                            FROM adventureworks2008r2_humanresources.department p \
                            LEFT JOIN adventureworks2008r2_humanresources.employee_department_history e ON p.department_id=e.department_id \
                            GROUP BY p.department_id,p.name,p.group_name;")
            
            #Retrieve the query result set
            
            query = cursor.fetchall()
            connect.commit()
            if len(query) == 0:
                respones = {
                'data': None
            }
                return Response(data=respones, status=status.HTTP_404_NOT_FOUND)
            df = pd.DataFrame(query)
            df.columns = ['department_id', 'name', 'group_name', 'employess']
            result = df.to_json(orient="records")
            respones = {
                'data': json.loads(result)
            }
            _logger.info('End Get departments')

            return Response(data=respones, status=status.HTTP_200_OK)
        except Exception:
            return HttpResponseRedirect(reverse('user_logout'))

@permission_classes([IsAuthenticated])
def delete_employee(request, id):
    try:
        _logger.info('Start Delete detail employee')
        # Create a Cursor object
        _logger.info('Connect to Redshif')
    
        try:
            connect = Redshift().conn
            cursor = connect.cursor()
            cursor.execute(f"DELETE FROM adventureworks2008r2_humanresources.employee \
            WHERE business_entity_id={id};")

            cursor.execute(f"DELETE FROM adventureworks2008r2_humanresources.employee_department_history \
            WHERE business_entity_id={id};")
            connect.commit()

        except redshift_connector.error.ProgrammingError :
            messages.info(request, 'Failed!')
            return HttpResponseRedirect(reverse('employees'))
        messages.info(request, 'Delete successfully!')
        return HttpResponseRedirect(reverse('employees'))
    except Exception:
        return HttpResponseRedirect(reverse('user_logout'))

@permission_classes([IsAuthenticated])
def update_employee_department(request, id):
    # if this is a POST request we need to process the form data
    try:
        connect = Redshift().conn
        cursor = connect.cursor()
        if request.method == 'POST':
            # create a form instance and populate it with data from the request:
            form = DepartmentForm(request.POST)
            
            # check whether it's valid:
            if form.is_valid():
                
                query = f"SELECT department_id \
                        FROM adventureworks2008r2_humanresources.department \
                        WHERE name='{form.cleaned_data['department']}';"

                cursor.execute(query)

                result = cursor.fetchall()
                value = ','.join(f"{key}='{form.cleaned_data[key]}'" for key in form.cleaned_data if key!='department')
                value = value + f',department_id={result[0][0]}'
                value = value.replace("'None'", "null ")
                query = f"UPDATE adventureworks2008r2_humanresources.employee_department_history \
                        SET {value} \
                        WHERE business_entity_id={id} AND modified_date=(SELECT max(modified_date) FROM adventureworks2008r2_humanresources.employee_department_history \
                        WHERE business_entity_id={id});"
                cursor.execute(query)
                connect.commit()

                messages.info(request, 'Update sucessfully')
                return HttpResponseRedirect(reverse('detai_employee',args=(id,)))
            else:
                messages.info(request, 'Can not update data (data invalid)')
                return HttpResponseRedirect(reverse('detai_employee',args=(id,)))
        
        # if a GET (or any other method) we'll create a blank form
        elif request.method == 'GET':
            # Query a table using the Cursor
            cursor.execute(f"SELECT p.business_entity_id,p.first_name,p.last_name,e.birth_date,e.salaried_flag,e.vacation_hours,e.sick_leave_hours,e.hire_date,e.gender,e.job_title,dt.name,d.start_date,d.end_date \
                            FROM adventureworks2008r2_person.person p \
                            INNER JOIN adventureworks2008r2_humanresources.employee e ON p.business_entity_id=e.business_entity_id \
                            INNER JOIN adventureworks2008r2_humanresources.employee_department_history d ON p.business_entity_id=d.business_entity_id \
                            INNER JOIN adventureworks2008r2_humanresources.department dt ON d.department_id=dt.department_id    \
                            WHERE p.business_entity_id={id};")
            
            query = cursor.fetchall()
            connect.commit()
            if len(query) == 0:
                return HttpResponseRedirect(reverse('detai_employee',args=(id,)))
            df = pd.DataFrame(query)
            df.columns = ['business_entity_id', 'first_name', 'last_name', 'birth_date', 'salaried_flag', 'vacation_hours', 'sick_leave_hours', 'hire_date', 'gender', 'job_title', 'name', 'start_date', 'end_date']
            df['birth_date'] = pd.to_datetime(df['birth_date']).astype(str)
            df['hire_date'] = pd.to_datetime(df['hire_date']).astype(str)
            df['start_date'] = pd.to_datetime(df['start_date']).astype(str)
            df['end_date'] = pd.to_datetime(df['end_date']).astype(str)

            result = json.loads(df.to_json(orient="records"))[0]    
            form_data = {
                "name": result['name'],
                "start_date": result['start_date'],
                "end_date": result['end_date'],
            }
            
            form_data = DepartmentForm(form_data)

            return render(request, 'department_employee_form.html', {'form': form_data, 'id':id})
   
    except Exception:
        return HttpResponseRedirect(reverse('user_logout'))

@permission_classes([IsAuthenticated])
def create_department(request):
    # if this is a POST request we need to process the form data
    try:
        connect = Redshift().conn
        cursor = connect.cursor()
        if request.method == 'POST':
            # create a form instance and populate it with data from the request:
            form = CreateDepartmentForm(request.POST)
            
            # check whether it's valid:
            if form.is_valid():
        
                query = f"INSERT INTO adventureworks2008r2_humanresources.department (name, group_name, modified_date) \
                        VALUES ('{form.cleaned_data['name']}', '{form.cleaned_data['group_name']}', '{datetime.datetime.now()}');"
                cursor.execute(query)
                connect.commit()

                messages.info(request, 'Create sucessfully')
                return HttpResponseRedirect(reverse('departments'))
            else:
                messages.info(request, 'Can not update data (data invalid)')
                return HttpResponseRedirect(reverse('departments'))
        
        # if a GET (or any other method) we'll create a blank form
        elif request.method == 'GET':
            # Query a table using the Cursor
            form_data = CreateDepartmentForm()

            return render(request, 'create_department.html', {'form': form_data})
    except Exception:
        return HttpResponseRedirect(reverse('user_logout'))

@permission_classes([IsAuthenticated])
def create_department(request):
    # if this is a POST request we need to process the form data
    try:
        connect = Redshift().conn
        cursor = connect.cursor()
        if request.method == 'POST':
            # create a form instance and populate it with data from the request:
            form = CreateDepartmentForm(request.POST)
            
            # check whether it's valid:
            if form.is_valid():
        
                query = f"INSERT INTO adventureworks2008r2_humanresources.department (name, group_name, modified_date) \
                        VALUES ('{form.cleaned_data['name']}', '{form.cleaned_data['group_name']}', '{datetime.datetime.now()}');"
                cursor.execute(query)
                connect.commit()

                messages.info(request, 'Create sucessfully')
                return HttpResponseRedirect(reverse('departments'))
            else:
                messages.info(request, 'Can not update data (data invalid)')
                return HttpResponseRedirect(reverse('departments'))
        
        # if a GET (or any other method) we'll create a blank form
        elif request.method == 'GET':
            # Query a table using the Cursor
            form_data = CreateDepartmentForm()

            return render(request, 'create_department.html', {'form': form_data})
    except Exception:
        return HttpResponseRedirect(reverse('user_logout'))