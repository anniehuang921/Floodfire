from django.shortcuts import render
from datetime import datetime
from django.http import HttpResponse
from django.http import HttpResponseRedirect

from django.shortcuts import render_to_response
from django.template.defaulttags import register
from django.template import RequestContext
from elasticsearch1.models import Esquery

from elasticsearch1.esclass import esf, esc

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
# Create your views here.
def query_list(request):
    queries = Esquery.objects.all()
    return render_to_response('result',locals())

def result(request):
    if request.GET:
        return render(request,'ui_first.html')
    else:
        return render_to_response('ui_first.html')
#
@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)
#
def ui_first(request):
    # if request.get:
    #     return render_to_response('ui_first.html')
    if request.POST:
        allq = request.POST['allq']
        exact = request.POST['exact']
        least = request.POST['least']
        notq = request.POST['notq']
        # source1 = request.POST['source1']
        # author = request.POST['author']
        # occur = request.POST['occur']
        # for i in range(4):
            # exec("%s = %s" % (media+i,request.POST['media'+str(i)]))

        # media = [request.POST["media0"],request.POST["media1"],request.POST["media2"],request.POST["media3"]]
        media = request.POST.getlist('media')
        sort = request.POST['sort']
        if sort == "時間":
            sort = True
        else:
            sort = False

        # date1 = request.POST['date1']
        # date2 = request.POST['date2']
        Esquery.objects.create(
            allq = allq,
            exact = exact,
            least = least,
            notq = notq,
            # source1 = source1,
            # author = author,
            # occur = occur,
            media = media,
            sort = sort,
            # date1 = date1,
            # date2 = date2
        )

        QU = esc(media ,
            must_cond = exact,
            mustnot_cond = notq,
            should_cond = least,
            time_sort = sort,
            # date1 = date1, date2 = date2 )
            date1 = None, date2 = None )
        count = QU.count()
        result = QU.result(3)
        # search = QU.search()




#     if request.GET.get('allq','')!="" :#or
#     #     request.GET.get('exact','')!="" or
#     #     request.GET.get('least','')!="" or
#     #     request.GET.get('not','')!="" or
#     #     request.GET.get('source','')!="" or
#     #     request.GET.get('author','')!="" or
#     #     request.GET.get('occur','')!="" or
#     #     request.GET.get('media','')!="" or
#     #     request.GET.get('notq','')!="" or:
#
# [allq, exact, least, notq, media, sort, date1, date2]
#         one = request.GET.get('allq','')
#         media = request.GET.get('media','')
#
#          ES = esf(["ptt","facebook"])
#          doc_type=ES.doc_type
#          QU = esc(media,must_cond=exact,mustnot_cond=notq,should_cond=least.append(allq))
#         # two = request.GET.get('second','')
#         # total= u"柯"
#
#
        if len(result)>0:
            results = {
                # "one":one,
                "one":"test",

                "count":count,
                "field":['platform','media_name','title','from_user_name','from_user_nick', 'content', 'time'],
                "name":{'media_name':"版名",
                        'from_user_name':"發文者名稱",
                        'from_user_nick':"發文者暱稱",
                        'title': "標題",
                        'content':"內容",
                        'time': "發文時間",
                        'platform':"媒體來源"},
                "events":result,

                "result":result[0]['_source'].keys()}

                # "content":result['hits']['hits'][0]['_source']['content'],
                # 'media_name':result['hits']['hits'][0]['_source']['media_name'],
                # 'from_user_name':result['hits']['hits'][0]['_source']['from_user_name'],
                # 'from_user_nick':result['hits']['hits'][0]['_source']['from_user_nick'],
                # 'title':result['hits']['hits'][0]['_source']['title'],
                # 'content':result['hits']['hits'][0]['_source']['content'],
                # 'time':result['hits']['hits'][0]['_source']['time'],
                # 'platform':result['hits']['hits'][0]['_source']['platform'],

        else:
                results = {
                "one":one,
                "result":'無查詢結果',
                "content":"",
            }

        #return HttpResponse('Welcome!~'+ str(total))
        #return HttpResponse(result['hits']['hits'][0]['_source']['title'])
        # return render(request,"result.html",results)
        # return render_to_response('result.html',RequestContext(request,locals()))
        return render_to_response('result.html',RequestContext(request,results))

    else:
        return render_to_response('ui_first.html',RequestContext(request))
#     else:
#         return render_to_response('ui_first.html')#,locals())
#
#     # if request.POST:
#     #     visitor = request.POST['visitor']
#     #     content = request.POST['content']
#     #     email = request.POST['email']
#     #     date_time = datetime.datetime.now()
#     #     if '@' not in email:
#     #         errors.append('*email 格式不正確，請重新輸入')
#     #     if not errors:
#     #         Comment.objects.create(visitor= visitor,email=email,
#     #             content=content,date_time=date_time)
#     #         visitor, email, content = ('','','')
#     #     f = NameForm()
#     #     return render('testapp.html',RequestContext(request))
#     #
#
#
#
# # def testapp(request,id):
#     # if id:
#     #     r = Restaurant.objects.get(id=id)
#     # else:
#     #     return HttpResponseRedirect("/testapp/")
#
#
#
#     # title = "Welcome"
#     # if this is a POST request we need to process the form data
#
#
#
#
#     # if request.method == 'POST':
#
#         # create a form instance and populate it with data from the request:
#         # form = NameForm(request.POST or None)
#         # check whether it's valid:
#         # if form.is_valid():
#         #     # process the data in form.cleaned_data as required
#         #     # ...
#         #     # redirect to a new URL:
#         #
#         #     # instance = form.save(commit=False)
#         #     # form.save()
#         #     # print (form)
#         #
#         #     return HttpResponseRedirect('/thanks/')
#
#     # if a GET (or any other method) we'll create a blank form
#     # else:
#     #     form = NameForm()
#     # context = {
#     #     "title" : title,
#     #     "form" : form,
#     #     }
#
#     # return render(request, 'testapp.html', context)
