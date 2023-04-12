from django.urls import path

from .views import (DepartmentView, DetailEmployeeView, EmployeeView,
                    alert_delete, delete_employee, update_employee,
                    update_employee_department, create_department)

urlpatterns = [

    path('humanresources/employees', EmployeeView.as_view(), name='employees'),
    path('humanresources/employees/<int:id>', DetailEmployeeView.as_view(), name='detai_employee'),
    path('humanresources/employees/<int:id>/delete', delete_employee, name='delete_employee'),
    path('humanresources/employees/<int:id>/alert', alert_delete, name='alert_delete_employee'),
    path('humanresources/employees/<int:id>/form', update_employee, name='form_employee'),
    path('humanresources/employees-department/<int:id>/form', update_employee_department, name='form_department_employee'),
    path('humanresources/departments', DepartmentView.as_view(), name='departments'),
    path('humanresources/departments/create', create_department, name='create_department'),
]