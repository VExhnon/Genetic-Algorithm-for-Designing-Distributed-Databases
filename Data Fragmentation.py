import psycopg2
import random


def connect_to_postgresql():
    try:
        # Установка соединения с базой данных PostgreSQL
        connection = psycopg2.connect(
            host="127.0.0.2",  # Адрес хоста базы данных PostgreSQL
            port="5432",  # Порт базы данных PostgreSQL
            database="testdbmaster",  # Имя базы данных PostgreSQL
            user="postgres",  # Имя пользователя базы данных PostgreSQL
            password="123"  # Пароль пользователя базы данных PostgreSQL
        )

        # Создание курсора для выполнения операций с базой данных
        cursor = connection.cursor()

        # Вывод успешного сообщения о подключении
        print("Успешное подключение к базе данных PostgreSQL")

        # Возвращаем объект соединения и курсора
        return cursor

    except (Exception, psycopg2.Error) as error:
        # Вывод сообщения об ошибке, если не удалось подключиться к базе данных
        print("Ошибка при подключении к базе данных PostgreSQL:", error)
        return None, None


def get_tables(cursor):
    try:
        # Выполнение SQL-запроса для получения списка таблиц
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

        # Извлечение результатов запроса
        tables = cursor.fetchall()

        # Вывод списка таблиц
        print("Список таблиц в базе данных:")
        for table in tables:
            print(table[0])

    except (Exception, psycopg2.Error) as error:
        # Вывод сообщения об ошибке, если не удалось получить список таблиц
        print("Ошибка при получении списка таблиц:", error)


def fragment_data():
    # Параметры подключения к базе данных
    db_host = '127.0.0.2'
    db_port = '5432'
    db_name = 'testdbmaster'
    db_user = 'postgres'
    db_password = '123'

    # Функция создания начальной популяции
    def generate_initial_population(num_nodes, num_rows, num_initial_population):
        initial_population = []
        for _ in range(num_initial_population):
            chromosome = []
            for _ in range(num_rows):
                group = random.randint(1, num_nodes)  # Определение фрагмента для каждой строки
                chromosome.append(group)
            initial_population.append(chromosome)
        return initial_population

    def get_publisher_id_by_row_number(row_number):
        conn = psycopg2.connect(database="testdbmaster", user="postgres", password="123", host="127.0.0.2",
                                port="5432")
        cur = conn.cursor()
        cur.execute(
            "SELECT PublisherID FROM (SELECT PublisherID, ROW_NUMBER() OVER (ORDER BY ISBN) as rn FROM Book) AS subquery WHERE rn = %s;",
            (row_number,))
        publisher_id = cur.fetchone()[0]
        cur.close()
        conn.close()
        return publisher_id

    # Функция расчета функции приспособленности
    def fitness_function(chromosome, table_size, fragment_size):
        fitness = 0
        fragments = []
        fragment_loads = []
        fragment_sizes = []

        # Создание фрагментов на основе хромосомы
        for gene in chromosome:
            if gene not in fragments:
                fragments.append(gene)
                fragment_loads.append(1)
            else:
                index = fragments.index(gene)
                fragment_loads[index] += 1

        # Вычисление приспособленности
        for load in fragment_loads:
            # Приспособленность для равномерного распределения нагрузки
            fitness += abs(load - table_size / fragment_size)

        # Приспособленность для локальности связанных данных
        i = 0
        for gene in fragments:
            i = i + 1
            if gene != None:
                publisher_id = get_publisher_id_by_row_number(i)  # Получить PublisherID из гена
                books_in_fragment = [book for book in fragments if get_publisher_id_by_row_number(book) == publisher_id]
                if len(books_in_fragment) > 1:
                    fitness -= 1

        return fitness

    # Функция фрагментации данных с помощью генетического алгоритма
    def fragment_data_using_genetic_algorithm(num_nodes, num_rows, num_initial_population, num_generations):
        # Шаг 1: Создание начальной популяции
        initial_population = generate_initial_population(num_nodes, num_rows, num_initial_population)
        fragments = []

        for generation in range(num_generations):
            # Шаг 2: Расчет значения функции приспособленности для каждой хромосомы в популяции
            fitness_scores = [fitness_function(chromosome, num_rows, num_rows) for chromosome in initial_population]

            # Шаг 3: Выбор родителей для скрещивания (селекция)
            # Наиболее приспособленные хромосомы имеют больший шанс быть выбранными
            selected_parents = random.choices(initial_population, weights=fitness_scores, k=num_initial_population)

            # Шаг 4: Скрещивание родителей для создания потомства (кроссовер)
            # В данном случае используется одноточечный кроссовер
            offspring = []
            for i in range(num_initial_population):
                parent1 = selected_parents[i]
                parent2 = selected_parents[(i + 1) % num_initial_population]
                crossover_point = random.randint(1, num_rows - 1)
                child = parent1[:crossover_point] + parent2[crossover_point:]
                offspring.append(child)

            # Шаг 5: Мутация потомства
            # В данном случае используется мутация с вероятностью 0.01 для каждого гена
            mutated_offspring = []
            for child in offspring:
                mutated_child = [gene if random.random() > 0.01 else random.randint(1, num_nodes) for gene in child]
                mutated_offspring.append(mutated_child)

            # Шаг 6: Замена старой популяции новым потомством
            initial_population = mutated_offspring

            # Шаг 7: Оценка лучшей хромосомы текущего поколения
            best_chromosome = max(mutated_offspring, key=lambda x: fitness_function(x, num_rows, num_rows))


            # Добавление текущего лучшего разбиения в список фрагментов
            fragments.append(best_chromosome)

        return fragments

    def get_num_rows():
        # Установка параметров подключения к базе данных
        db_host = '127.0.0.2'
        db_name = 'testdbmaster'
        db_user = 'postgres'
        db_password = '123'

        # Подключение к базе данных
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )

        # Создание объекта курсора
        cursor = conn.cursor()

        try:
            # Выполнение SQL-запроса для получения количества строк в таблице BOOK
            cursor.execute("SELECT COUNT(*) FROM BOOK")

            # Получение результата запроса
            num_rows = cursor.fetchone()[0]

            return num_rows

        except (Exception, psycopg2.Error) as error:
            print("Ошибка при выполнении SQL-запроса:", error)

        finally:
            # Закрытие курсора и соединения с базой данных
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    # Задание параметров и запуск фрагментации данных
    num_nodes = 3  # Количество фрагментов (узлов)
    num_rows = get_num_rows()
    num_initial_population = 10  # Размер начальной популяции
    num_generations = 1  # Количество поколений

    fragments = fragment_data_using_genetic_algorithm(num_nodes, num_rows, num_initial_population, num_generations)
    fragments_buff = fragments[0]
    return (fragments_buff)

def get_fragments(fragments):
    d = {a: fragments[a - 1] for a in range(1, 151)}

    sorted_rooms = dict(sorted(d.items(), key=lambda d: d[1]))

    fragment_count1 = 0
    fragment_count2 = 0
    fragment_count3 = 0
    for i in range(1, len(sorted_rooms) + 1):
        if sorted_rooms[i] == 1:
            fragment_count1 += 1
        elif sorted_rooms[i] == 2:
            fragment_count2 += 1
        else:
            fragment_count3 += 1

    fragment1 = []
    fragment2 = []
    fragment3 = []

    aaaa = sorted_rooms.items()
    for a, b in aaaa:
        if b == 1:
            fragment1.append(a)
        elif b == 2:
            fragment2.append(a)
        else:
            fragment3.append(a)

    return fragment1, fragment2, fragment3



def main():
    # Подключение к базе данных PostgreSQL
    cursor = connect_to_postgresql()

    # Создание распределенной базы данных
    get_tables(cursor)

    # Фрагментация данных
    fragments = fragment_data()

    frgmt1, frgmt2, frgmt3 = get_fragments(fragments)

    print(frgmt1, "\n", frgmt2, "\n", frgmt3)

    # Точка входа в программу
if __name__ == '__main__':
    main()
