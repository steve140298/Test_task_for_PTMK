import sqlite3 as sq
import datetime as dt
import random
import time
import sys
from sys import argv
from russian_names import RussianNames


class DataBase:
    def __init__(self):
        # creating and open database
        with sq.connect('base.db') as self.__con:
            self.__cursor = self.__con.cursor()

    # switch 1
    def create_table(self):
        try:
            self.__cursor.execute('''CREATE TABLE employee_directory
                          (fullname TEXT, date_of_birth TEXT, gender TEXT)''')
            print('The employee directory table has been successfully created!')
            self.__con.close()
        except sq.OperationalError:
            print('The employee directory table already exists!')
            self.__con.close()

    # switch 3
    def show_all_data(self):
        try:
            start_time = time.time()

            self.__cursor.execute('SELECT * FROM employee_directory GROUP BY fullname, date_of_birth ORDER BY fullname')
            data = self.__cursor.fetchall()
            end_time = time.time()
            print(f'Query completed in ', end_time-start_time, 's')

            # creating beautiful table display
            for row in data:
                print('-'*80)
                age = Employee.full_age(row[1])
                print(*row, f'{age} years old')
            self.__con.close()
        except sq.OperationalError:
            print('The table does not exist! Please create a new one!')
            self.__con.close()

    # switch 4
    def auto_insert(self, quantity_lines=1000000):
        array_obj = []
        start_time = time.time()

        # creating fullnames
        men = RussianNames(count=1000, transliterate=True, gender=1).get_batch()
        women = RussianNames(count=1000, transliterate=True, gender=0).get_batch()

        # creating 1 million objects
        for i in range(quantity_lines):

            # creating random date
            year = random.randint(1958, 2004)
            month = random.randint(1, 12)
            day = random.randint(1, 28)
            date = f'{year}-{month}-{day}'

            # random gender 0-Male 1-Female
            coin = random.randint(0, 1)

            if coin == 0:
                names = random.choice(men)
                names = names.split(' ')
                names = names[2] + ' ' + names[0] + ' ' + names[1]
                employee_info = [names, date, 'Male']
            else:
                names = random.choice(women)
                names = names.split(' ')
                names = names[2] + ' ' + names[0] + ' ' + names[1]
                employee_info = [names, date, 'Female']

            # creating employee
            employee = Employee(employee_info)

            # add obj into the array
            array_obj.append(employee)

        end_time = time.time()
        print(' The creation of an array of objects was completed in', end_time - start_time)
        self.send_array_obj(array_obj)
        self.__auto_insert_f_fullname()
        self.__con.close()

    # creation of 100 men starting with F
    def __auto_insert_f_fullname(self):

        men = RussianNames(count=10000, transliterate=True, gender=1).get_batch()
        counting = 0
        array_obj = []

        for m in men:
            m = m.split(' ')
            if m[2][0] == 'F' and counting != 100:
                year = random.randint(1958, 2004)
                month = random.randint(1, 12)
                day = random.randint(1, 28)
                date = f'{year}-{month}-{day}'
                men_fullnames = m[2] + ' ' + m[0] + ' ' + m[1]
                employee_info = [men_fullnames, date, 'Male']
                employee = Employee(employee_info)
                array_obj.append(employee)
                counting += 1
            elif counting == 100:
                break

        self.send_array_obj(array_obj)

    # getting array of objects and send them to base
    def send_array_obj(self, array_obj=[]):

        self.__cursor.execute("DROP INDEX IF EXISTS idx_fullname")
        start_time = time.time()
        # Begin transaction
        start_time = time.time()

        for obj in array_obj:

            obj_tuple = (obj.fullname, obj.date_of_birth.date(), obj.gender)
            self.__cursor.execute("INSERT INTO employee_directory (fullname, date_of_birth, gender) "
                                "VALUES(?, ?, ?)", obj_tuple)
        self.__con.commit()
        end_time = time.time()
        print(f'The time of the request to insert {len(array_obj)} objects into the database was', end_time-start_time, 's')

    # switch 5
    def show_info_male_f(self):

        start = time.time()

        # creating index
        self.__cursor.execute("CREATE INDEX IF NOT EXISTS idx_fullname ON employee_directory(fullname COLLATE NOCASE);")
        start_query = int(round(time.time() * 1000))

        self.__cursor.execute("SELECT * FROM employee_directory WHERE gender='Male' AND fullname LIKE ('F%')")

        data = self.__cursor.fetchall()
        end_query = int(round(time.time() * 1000))

        for row in data:
            print('-' * 80)
            print(*row)

        end = time.time()
        print()
        print('The query execution time was', end_query-start_query, 'ms')
        print('The program execution time was', end-start, 's')
        print(len(data), 'rows detected')
        self.__con.close()


class Employee:
    def __init__(self, employee_data):

        try:
            self.fullname = employee_data[0]
            self.date_of_birth = dt.datetime.strptime(employee_data[1], '%Y-%m-%d')
            self.gender = employee_data[2]

        except ValueError:
            print("Incorrect date entry. The date should be entered this way: '%Y-%m-%d'")
            sys.exit()

    # switch 2
    def add_employee_info(self):
        try:
            with sq.connect('base.db') as self.__con:
                self.__cursor = self.__con.cursor()
            self.__cursor.execute("INSERT INTO employee_directory (fullname, date_of_birth, gender) "
                                "VALUES(?, ?, ?)", (self.fullname, self.date_of_birth.date(), self.gender)
                                )
            self.__con.commit()
            print('The record was created successfully!')
        except ValueError:
            print('Enter your full name, Date of birth, Gender')
            self.__con.close()

    @staticmethod
    def full_age(birthday=''):
        try:
            birthday = dt.datetime.strptime(birthday, '%Y-%m-%d')
            date_time_obj = dt.datetime.today()
            age = date_time_obj - birthday
            age = age.days // 365
            return age
        except TypeError:
            date_time_obj = dt.datetime.today()
            age = date_time_obj.date() - birthday
            age = age.days // 365
            return age


if __name__ == "__main__":

    # Getting option from terminal
    try:
        switch = argv[1]
    except:
        print('Enter the arguments!')
        switch = 0

    # creating database
    base = DataBase()

    # operating modes
    if switch == '1':
        if len(argv) == 2:
            base.create_table()
        else:
            print('Arguments entered incorrectly! Expected 1 argument')

    elif switch == '2':
        if len(argv) == 5:
            employee = Employee(argv[2:])
            employee.add_employee_info()

        else:
            print('Arguments entered incorrectly! Expected  4 arguments')

    elif switch == '3':
        if len(argv) == 2:
            base.show_all_data()
        else:
            print('Arguments entered incorrectly! Expected 1 argument')
    elif switch == '4':
        if len(argv) == 2:
            base.auto_insert()
        else:
            print('Arguments entered incorrectly! Expected 1 argument')
    elif switch == '5':
        if len(argv) == 2:
            base.show_info_male_f()
        else:
            print('Arguments entered incorrectly! Expected 1 argument')








