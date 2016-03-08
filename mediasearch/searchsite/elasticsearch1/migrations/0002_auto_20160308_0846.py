# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('elasticsearch1', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='esquery',
            name='date1',
            field=models.DateTimeField(),
        ),
        migrations.AlterField(
            model_name='esquery',
            name='date2',
            field=models.DateTimeField(),
        ),
    ]
