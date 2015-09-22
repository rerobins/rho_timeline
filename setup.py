from distutils.core import setup

setup(
    name='timeline_bot',
    version='1.0.0',
    packages=['timeline_bot',
              'timeline_bot.components',
              'timeline_bot.components.commands',
              ],
    url='',
    license='BSD',
    author='Robert Robinson',
    author_email='rerobins@meerkatlabs.org',
    description='Timeline for the Rho infrastructure',
    install_requires=['rhobot==1.0.0', ]
)
