# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Esquery',
            fields=[
                ('id', models.AutoField(primary_key=True, verbose_name='ID', auto_created=True, serialize=False)),
                ('allq', models.CharField(max_length=50)),
                ('exact', models.CharField(max_length=50)),
                ('least', models.CharField(max_length=50)),
                ('notq', models.CharField(max_length=50)),
                ('source', models.CharField(max_length=50)),
                ('author', models.CharField(max_length=50)),
                ('occur', models.CharField(max_length=50)),
                ('media', models.CharField(max_length=50)),
                ('sort', models.CharField(max_length=50)),
                ('date1', models.CharField(max_length=50)),
                ('date2', models.CharField(max_length=50)),
            ],
        ),
    ]
