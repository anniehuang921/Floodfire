from django import forms
# from django.forms import ModelForms

class NameForm(forms.Form):
    your_name = forms.CharField(label='Your name', max_length=100)
    subject = forms.CharField(max_length=100)
    message = forms.CharField(widget=forms.Textarea)
    sender = forms.EmailField()
    cc_myself = forms.BooleanField(required=False)

    # visitor = forms.CharField(max_length=20)
    # email = forms.EmailField(max_length=20,required=False)
    # content = forms.CharField(max_length=200,widget=forms.Textarea())
