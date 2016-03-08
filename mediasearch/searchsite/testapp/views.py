from django.shortcuts import render
from datetime import datetime
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from .forms import NameForm
from django.shortcuts import render_to_response

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
es_index=IndicesClient(es)
result= es.search(index="platform",doc_type="ptt",body={"size":3,"query":{"match":{u"content":u"柯"}}})

# Create your views here.
# def result(request):
#     if request.GET:
#         return render(request,'testapp.html')
#     else:
#         return render(request,'result.html')

def testapp(request):
    if request.GET.get('first','')!="" :
        one = request.GET.get('first','')
        # two = request.GET.get('second','')
        # total= u"柯"

        es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        es_index=IndicesClient(es)
        result= es.search(index="platform",doc_type="ptt",body={"size":3,"query":{"match":{u"content":one}}})
        results = {
            "one":one,
            "result":result['hits']['hits'][0]['_source']['title'],
        }
        #return HttpResponse('Welcome!~'+ str(total))
        #return HttpResponse(result['hits']['hits'][0]['_source']['title'])
        return render(request,"result.html",results)
    else:
        return render_to_response('testapp.html',locals())

    # if request.POST:
    #     visitor = request.POST['visitor']
    #     content = request.POST['content']
    #     email = request.POST['email']
    #     date_time = datetime.datetime.now()
    #     if '@' not in email:
    #         errors.append('*email 格式不正確，請重新輸入')
    #     if not errors:
    #         Comment.objects.create(visitor= visitor,email=email,
    #             content=content,date_time=date_time)
    #         visitor, email, content = ('','','')
    #     f = NameForm()
    #     return render('testapp.html',RequestContext(request))
    #



# def testapp(request,id):
    # if id:
    #     r = Restaurant.objects.get(id=id)
    # else:
    #     return HttpResponseRedirect("/testapp/")



    # title = "Welcome"
    # if this is a POST request we need to process the form data




    # if request.method == 'POST':

        # create a form instance and populate it with data from the request:
        # form = NameForm(request.POST or None)
        # check whether it's valid:
        # if form.is_valid():
        #     # process the data in form.cleaned_data as required
        #     # ...
        #     # redirect to a new URL:
        #
        #     # instance = form.save(commit=False)
        #     # form.save()
        #     # print (form)
        #
        #     return HttpResponseRedirect('/thanks/')

    # if a GET (or any other method) we'll create a blank form
    # else:
    #     form = NameForm()
    # context = {
    #     "title" : title,
    #     "form" : form,
    #     }

    # return render(request, 'testapp.html', context)
