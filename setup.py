from setuptools import setup

package_name = 'necst'

setup(
    name=package_name,
    version='0.1.0',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='Kaoru Nishikawa',
    maintainer_email='k.nishikawa@a.phys.nagoya-u.ac.jp',
    description='NEw Control System for Telescope',
    license='MIT',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            f"start={package_name}.tempos.ros2_string_pub:main"
        ],
    },
)
