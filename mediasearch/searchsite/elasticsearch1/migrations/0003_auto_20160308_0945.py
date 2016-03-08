# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('elasticsearch1', '0002_auto_20160308_0846'),
    ]

    operations = [
        migrations.AlterField(
            model_name='esquery',
            name='allq',
            field=models.CharField(blank=True, max_length=50),
        ),
        migrations.AlterField(
            model_name='esquery',
            name='author',
            field=models.CharField(blank=True, max_length=50),
        ),
        migrations.AlterField(
            model_name='esquery',
            name='date1',
            field=models.DateTimeField(null=True, blank=True),
        ),
        migrations.AlterField(
            model_name='esquery',
            name='date2',
            field=models.DateTimeField(null=True, blank=True),
        ),
        migrations.AlterField(
            model_name='esquery',
            name='exact',
            field=models.CharField(blank=True, max_length=50),
        ),
        migrations.AlterField(
            model_name='esquery',
            name='least',
            field=models.CharField(blank=True, max_length=50),
        ),
        migrations.AlterField(
            model_name='esquery',
            name='media',
            field=models.CharField(blank=True, max_length=50),
        ),
        migrations.AlterField(
            model_name='esquery',
            name='notq',
            field=models.CharField(blank=True, max_length=50),
        ),
        migrations.AlterField(
            model_name='esquery',
            name='occur',
            field=models.CharField(blank=True, max_length=50),
        ),
        migrations.AlterField(
            model_name='esquery',
            name='sort',
            field=models.CharField(blank=True, max_length=50),
        ),
        migrations.AlterField(
            model_name='esquery',
            name='source',
            field=models.CharField(blank=True, max_length=50),
        ),
    ]
