# -*- coding: utf-8 -*-
"""Public section, including homepage and signup."""
from flask import (Blueprint, request, render_template, flash, url_for,
                    redirect, session)
from flask_login import login_user, login_required, logout_user

from myflaskapp.extensions import login_manager
from myflaskapp.user.models import User
from myflaskapp.public.forms import LoginForm
from myflaskapp.user.forms import RegisterForm
from myflaskapp.utils import flash_errors
from myflaskapp.database import db
import datetime

blueprint = Blueprint('public', __name__, static_folder="../static")


@login_manager.user_loader
def load_user(id):
    return User.get_by_id(int(id))


@blueprint.route("/", methods=["GET", "POST"])
def home():
    day = datetime.datetime.now()
    previous_day = "%s-%s-%s" % (day.year, day.month, day.day - 1)
    form = previous_day
    return render_template("public/home.html", form=form)


@blueprint.route('/logout/')
@login_required
def logout():
    logout_user()
    flash('You are logged out.', 'info')
    return redirect(url_for('public.home'))


@blueprint.route("/register/", methods=['GET', 'POST'])
def register():
    form = RegisterForm(request.form, csrf_enabled=False)
    if form.validate_on_submit():
        new_user = User.create(username=form.username.data,
                        email=form.email.data,
                        password=form.password.data,
                        active=True)
        flash("Thank you for registering. You can now log in.", 'success')
        return redirect(url_for('public.home'))
    else:
        flash_errors(form)
    return render_template('public/register.html', form=form)


@blueprint.route("/about/")
def about():
    form = LoginForm(request.form)
    return render_template("public/about.html", form=form)
