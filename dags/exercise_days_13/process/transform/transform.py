from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import plotly.express as px
import os

class Transform:
    def __init__(self, DATA_PATH):
        self.path = DATA_PATH

    def transform_processing(self):
        ss = SparkSession.builder.appName('transform').master('spark://spark-master:7077').getOrCreate()

        # Create custom schema
        schema_course = StructType([
                        StructField('ID', IntegerType(), True),
                        StructField('NAME', StringType(), True)
                    ])
        schema_course_attendance = StructType([
                        StructField('ID', IntegerType(), True),
                        StructField('ATTEND_DT', StringType(), True),
                        StructField('SCHEDULE_ID', IntegerType(), True),
                        StructField('STUDENT_ID', IntegerType(), True),
                    ])
        schema_schedule = StructType([
                        StructField('COURSE_DAYS', StringType(), True),
                        StructField('COURSE_ID', IntegerType(), True),
                        StructField('END_DT', StringType(), True),
                        StructField('ID', IntegerType(), True),
                        StructField('LECTURER_ID', IntegerType(), True),
                        StructField('START_DT', StringType(), True),
                    ])
        schema_enrollment = StructType([
                        StructField('ACADEMIC_YEAR', StringType(), True),
                        StructField('ENROLL_DT', StringType(), True),
                        StructField('ID', IntegerType(), True),
                        StructField('SCHEDULE_ID', IntegerType(), True),
                        StructField('SEMESTER', IntegerType(), True),
                        StructField('STUDENT_ID', IntegerType(), True),
                    ])

        # Read data json base on custom schema
        course_schemadf = ss.read.schema(schema_course).json(self.path+'/1. extract/course-extracted.json')
        course_attendance_schemadf = ss.read.schema(schema_course_attendance).json(self.path+'/1. extract/course_attendance-extracted.json')
        schedule_schemadf = ss.read.schema(schema_schedule).json(self.path+'/1. extract/schedule-extracted.json')
        enrollment_schemadf = ss.read.schema(schema_enrollment).json(self.path+'/1. extract/enrollment-extracted.json')
        
        # Create temp view
        course_schemadf.createOrReplaceTempView('COURSE')
        course_attendance_schemadf.createOrReplaceTempView('COURSE_ATTENDANCE')
        schedule_schemadf.createOrReplaceTempView('SCHEDULE')
        enrollment_schemadf.createOrReplaceTempView('ENROLLMENT')
        all_data = ss.sql("""
                        select c.ID as COURSE_ID, c.NAME as COURSE_NAME, ca.ATTEND_DT, ca.STUDENT_ID, s.COURSE_DAYS, 
                        s.START_DT as SCHEDULE_START_DT, s.END_DT as SCHEDULE_END_DT, s.LECTURER_ID, e.ACADEMIC_YEAR
                        from COURSE c
                        inner join SCHEDULE s on s.COURSE_ID == c.ID
                        inner join COURSE_ATTENDANCE ca on ca.SCHEDULE_ID == s.ID
                        inner join ENROLLMENT e on e.STUDENT_ID == ca.STUDENT_ID
                        """)

        # Convert to date type
        all_data.withColumn('ATTEND_DT', F.to_date('ATTEND_DT','dd-MMM-yy'))
        all_data.withColumn('SCHEDULE_START_DT', F.to_date('SCHEDULE_START_DT','dd-MMM-yy'))
        all_data.withColumn('SCHEDULE_END_DT', F.to_date('SCHEDULE_END_DT','dd-MMM-yy'))
        
        # Group by for best of data
        BestOfCourse = all_data.groupBy('COURSE_NAME').agg(F.count('STUDENT_ID').alias('TOTAL_STUDENT')).orderBy('COURSE_NAME')
        BestOfAttendance = all_data.groupBy('ATTEND_DT').agg(F.count('STUDENT_ID').alias('TOTAL_STUDENT'))
        BestOfLecture = all_data.groupBy('LECTURER_ID').agg(F.count('STUDENT_ID').alias('TOTAL_STUDENT'))

        # Create plot dataframe
        BestOfCourseDF = BestOfCourse.toPandas()
        fig = px.line(BestOfCourseDF, x='COURSE_NAME', y='TOTAL_STUDENT')
        fig.write_image(f'{self.path}/2. transform/best_of_course.png')

        BestOfAttendanceDF = BestOfAttendance.toPandas()
        fig = px.bar(BestOfAttendanceDF, x='ATTEND_DT', y='TOTAL_STUDENT')
        fig.write_image(f'{self.path}/2. transform/best_of_attendance.png')

        BestOfLectureDF = BestOfLecture.toPandas()
        fig = px.pie(BestOfLectureDF, values='LECTURER_ID', names='TOTAL_STUDENT')
        fig.write_image(f'{self.path}/2. transform/best_of_lecture.png')

        # Create variable dictionary for all result data
        transformed_data = {
            'BestOfCourse': BestOfCourse,
            'BestOfAttendance': BestOfAttendance,
            'BestOfLecture': BestOfLecture
        }

        # Transform data to json
        for result_name, data in transformed_data.items():
            data.toPandas().to_json(f'{self.path}/2. transform/{result_name}.json', orient='records')
            print(f'Successfully transform data to json saved in {self.path}/2. transform/{result_name}.json')

        # Stop session spark
        ss.stop()
