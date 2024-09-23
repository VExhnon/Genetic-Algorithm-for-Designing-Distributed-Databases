import random
from faker import Faker

fake = Faker()

# Генерация данных для таблицы "Издательство"
publishers_data = [(i, fake.company(), fake.address()) for i in range(1, 151)]

authors_data = []
authors_date = []
# Генерация данных для таблицы "Автор
for i in range(1, 151):
    date = fake.date_of_birth()
    authors_data_buff = (i, fake.first_name(), fake.last_name())
    authors_data.append(authors_data_buff)
    authors_date.append(date)


# Генерация данных для таблицы "Книга"
books_data = []
isbn_set = set()
while len(books_data) < 150:
    isbn = fake.unique.isbn13()
    if isbn not in isbn_set:
        title = fake.sentence(nb_words=3)
        genre = random.choice(['Fiction', 'Non-Fiction', 'Mystery', 'Romance', "Horror"])
        publication_year = random.randint(1900, 2023)
        publisher_id = random.randint(1, 150)
        books_data.append((isbn, title, genre, publication_year, publisher_id))
        isbn_set.add(isbn)

# Вывод SQL запросов
print("-- Вставка данных в таблицу \"Издательство\"")
for data in publishers_data:
    print(f"INSERT INTO Publisher (PublisherID, Name, Address) VALUES {data};")

print("-- Вставка данных в таблицу \"Автор\"")
for i in range(150):
    print(f"INSERT INTO Author (AuthorID, FirstName, LastName, BirthDate) VALUES {authors_data[i]}'",authors_date[i],"';")

print("-- Вставка данных в таблицу \"Книга\"")
for data in books_data:
    print(f"INSERT INTO Book (ISBN, Title, Genre, PublicationYear, PublisherID) VALUES {data};")
