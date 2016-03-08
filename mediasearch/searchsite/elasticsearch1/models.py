from django.db import models

# Create your models here.
class Esquery(models.Model):
    allq=models.CharField(max_length=50)
    exact=models.CharField(max_length=50)
    least=models.CharField(max_length=50)
    notq=models.CharField(max_length=50)
    source=models.CharField(max_length=50)
    author=models.CharField(max_length=50)
    occur=models.CharField(max_length=50)
    media=models.CharField(max_length=50)
    sort=models.CharField(max_length=50)
    date1=models.CharField(max_length=50)
    date2=models.CharField(max_length=50)

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
