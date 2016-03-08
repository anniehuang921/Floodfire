from django.db import models

# Create your models here.
class Esquery(models.Model):
    allq=models.CharField(max_length=50,blank=True)
    exact=models.CharField(max_length=50,blank=True)
    least=models.CharField(max_length=50,blank=True)
    notq=models.CharField(max_length=50,blank=True)
    source=models.CharField(max_length=50,blank=True)
    author=models.CharField(max_length=50,blank=True)
    occur=models.CharField(max_length=50,blank=True)
    media=models.CharField(max_length=50,blank=True)
    sort=models.CharField(max_length=50,blank=True)
    # date1=models.DateTimeField(blank=True, null=True)
    # date2=models.DateTimeField(blank=True, null=True)

    # def __unicode__(self):
    #     return self.allq

# allq
# exact
# least
# notq
# source
# author
# occur
# media
# sort
# date1
# date2
