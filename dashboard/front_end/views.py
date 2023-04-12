from email.policy import HTTP
from django.shortcuts import render
from django.contrib.auth import authenticate, login, logout
from rest_framework.decorators import permission_classes
from rest_framework.permissions import IsAuthenticated
from django.http import HttpResponseRedirect
from django.urls import reverse
# Create your views here.
@permission_classes([IsAuthenticated])
def index(request):
  iframe = "https://app.powerbi.com/reportEmbed?reportId=03abea72-c903-43d3-8016-90bcd361c310&autoAuth=true&ctid=18791e17-6159-4f52-a8d4-de814ca8284a"
  context = {'iframe': iframe}
  return render(request, 'Dashboard.html', context)

@permission_classes([IsAuthenticated])
def home(request):
  iframe = "https://app.powerbi.com/reportEmbed?reportId=7dee1973-f2d5-4093-8b85-d7931e3ca7e2&autoAuth=true&ctid=7bbbced8-b31a-4a36-95bb-9f06bc9d72a6"
  context = {'iframe': iframe}
  return render(request, 'home.html', context)

def user_login(request):
    if request.method == 'POST':
        # Process the request if posted data are available
        username = request.POST['username']
        password = request.POST['password']
        # Check username and password combination if correct
        user = authenticate(username=username, password=password)
        if user is not None:
            # Save session as cookie to login the user
            login(request, user)
            # Success, now let's login the user.
            return HttpResponseRedirect(reverse('dashboard'))
        else:
            # Incorrect credentials, let's throw an error to the screen.
            return render(request, 'login.html', {'error_message': 'Incorrect username and / or password.'})
    else:
        # No post data availabe, let's just show the page to the user.
        return render(request, 'login.html')

def user_out(request):
    logout(request)
    return render(request, 'login.html')