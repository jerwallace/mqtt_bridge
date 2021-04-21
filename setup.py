from setuptools import find_packages
from setuptools import setup

package_name = 'mqtt_bridge'

setup(
    name=package_name,
    version='0.14.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        ('share/' + package_name + '/launch',
         ['launch/start.launch.py']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    author='Jeremy Wallace',
    author_email='jeremygw@amazon.com',
    maintainer='Jeremy Wallace',
    maintainer_email='jeremygw@amazon.com',
    keywords=['ROS'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Topic :: Software Development',
    ],
    description=(
        'Motor controllers for the jetbot '
    ),
    license='Apache License, Version 2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'mqtt_bridge = mqtt_bridge.app:main'
        ],
    },
)
