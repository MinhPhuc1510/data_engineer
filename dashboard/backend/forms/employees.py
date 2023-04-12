from django import forms
from backend.utils.redshif import Redshift

CHOICE_GENDER = [
    ('M', 'MALE'),
    ('F', 'FEMALE')
]
conn = Redshift().conn
def get_department():
        
        cursor = conn.cursor()
        cursor.execute("SELECT  p.name \
                                FROM adventureworks2008r2_humanresources.department p;")
        result = [(i[0],i[0]) for i in cursor.fetchall()]
        return result
GROUP_NAME = [
    ("Research and Development", "Research and Development"),
    ("Sales and Marketing", "Sales and Marketing"),
    ("Inventory Management", "Inventory Management"),
    ("Manufacturing", "Manufacturing"),
    ("Executive General and Administration", "Executive General and Administration"),
    ("Manufacturing", "Manufacturing"),
    ('Quality Assurance','Quality Assurance')

]

class EmployeeForm(forms.Form):
    first_name = forms.CharField(max_length=200)
    last_name = forms.CharField(max_length=200)
    birth_date = forms.DateField(widget=forms.SelectDateWidget(years=range(1900,3000)))
    gender = forms.CharField(widget=forms.Select(choices=CHOICE_GENDER))
    hire_date =  forms.DateField(widget=forms.SelectDateWidget(years=range(1900,3000),))
    job_title = forms.CharField(max_length=200)
    salary = forms.FloatField()

class DepartmentForm(forms.Form):
    department = forms.CharField(widget=forms.Select(choices=get_department()))
    start_date = forms.DateField(widget=forms.SelectDateWidget(years=range(1900,3000)))
    end_date = forms.DateField(widget=forms.SelectDateWidget(years=range(1900,3000)),required=False)


class CreateDepartmentForm(forms.Form):
    name = forms.CharField(max_length=200)
    group_name = forms.CharField(widget=forms.Select(choices=GROUP_NAME))

    