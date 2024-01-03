import sqlite3
import pandas as pd

# i assume my varian is 17

con = sqlite3.connect("library.sqlite")
f_damp = open('library.db', 'r', encoding='utf-8-sig')
damp = f_damp.read()
f_damp.close()
con.executescript(damp)

con.commit()
cursor = con.cursor()


cursor.execute('''
select b.book_id, b.title
from book b
where b.book_id in (
    select br.book_id
    from book_reader br)
''')
result = cursor.fetchall()
print(pd.DataFrame(result))
print('\n')

print('Task 1')
task1 = pd.read_sql('''
select b.book_id, title, 
    (select r.reader_name
    from reader r
    where r.reader_id =
        (select br.reader_id
         from book_reader br
         where b.book_id = br.book_id)) as reader_name,
    (select ((JulianDay(br.return_date) - JulianDay(br.borrow_date)) + 1)
    from book_reader br
    where b.book_id = br.book_id) as total_day
from book b
where total_day > 14
''', con)
print(task1)
print('\n')

print('Task 2')
task2 = pd.read_sql('''
SELECT
    b.title AS Title,
    GROUP_CONCAT(a.author_name, '\n') AS Author,
    p.publisher_name AS Publisher,
    b.year_publication AS Year
FROM
    book b
JOIN
    book_author ba ON b.book_id = ba.book_id
JOIN
    author a ON ba.author_id = a.author_id
JOIN
    publisher p ON b.publisher_id = p.publisher_id
GROUP BY
    b.book_id
ORDER BY
    b.title ASC,
    a.author_name ASC
''', con)
print(task2)
print('\n')

# purchase? borrow? what? googletranslate pls
print('Task 3')
task3 = pd.read_sql('''
WITH GenrePurchases AS (
    SELECT
        g.genre_id,
        g.genre_name,
        COUNT(*) AS total_purchases
    FROM
        genre g
    JOIN
        book b ON g.genre_id = b.genre_id
    JOIN
        book_reader br ON b.book_id = br.book_id
    GROUP BY
        g.genre_id, g.genre_name
)

SELECT
    b.title AS Title,
    a.author_name AS Author,
    g.genre_name AS Genre,
    p.publisher_name AS Publisher,
    b.year_publication AS Year
FROM
    book b
JOIN
    book_author ba ON b.book_id = ba.book_id
JOIN
    author a ON ba.author_id = a.author_id
JOIN
    genre g ON b.genre_id = g.genre_id
JOIN
    publisher p ON b.publisher_id = p.publisher_id
JOIN
    GenrePurchases gp ON g.genre_id = gp.genre_id
WHERE
    gp.total_purchases = (SELECT MAX(total_purchases) FROM GenrePurchases)
ORDER BY
    g.genre_name ASC,
    b.title ASC,
    a.author_name ASC;
''', con)
print(task3)
print('\n')


print('Task 4')
# what is this code.
# holy cow
task4 = sqlite3.connect("library.sqlite")
f_damp = open('library.db', 'r', encoding='utf-8-sig')
damp = f_damp.read()
f_damp.close()
task4.executescript(damp)

task4.commit()
cursors = task4.cursor()

try:
    # Find the last book borrowed by Reader Samarin S.S.
    last_borrowed_query = '''
        SELECT
            br.book_reader_id,
            br.borrow_date,
            br.return_date,
            b.title,
            b.available_numbers
        FROM
            reader r
        JOIN
            book_reader br ON r.reader_id = br.reader_id
        JOIN
            book b ON br.book_id = b.book_id
        WHERE
            r.reader_name = 'Samarin S.S.'
        ORDER BY
            br.borrow_date DESC
        LIMIT 1;
    '''

    cursor.execute(last_borrowed_query)
    last_borrowed_result = cursor.fetchone()

    if last_borrowed_result:
        book_reader_id, borrow_date, return_date, title, available_numbers = last_borrowed_result

        # Update book_reader with the return information
        update_book_reader_query = '''
            UPDATE
                book_reader
            SET
                return_date = CURRENT_DATE
            WHERE
                book_reader_id = ?;
        '''

        cursor.execute(update_book_reader_query, (book_reader_id,))
        task4.commit()

        # Update book with increased available_numbers
        update_book_query = '''
            UPDATE
                book
            SET
                available_numbers = ?
            WHERE
                book_id = ?;
        '''

        cursor.execute(update_book_query, (available_numbers + 1, book_reader_id))
        task4.commit()

except sqlite3.Error as e:
    # Handle any potential errors
    print("SQLite error:", e)

# Print the updated book table
print("\nUpdated Book Table:")
select_updated_book_query = '''
    SELECT * FROM book;
'''
cursor.execute(select_updated_book_query)
updated_book_results = cursor.fetchall()

for row in updated_book_results:
    print(row)

# Close the database connection
task4.close()

# i dont really understand the task, but how i understood this is how it should
print('Task 5')
task5 = pd.read_sql('''
WITH GenreBooks AS (
    SELECT
        g.genre_name AS Genre,
        b.title AS Book,
        ROW_NUMBER() OVER (PARTITION BY g.genre_id ORDER BY b.available_numbers ASC, b.title ASC) AS BookOrder,
        b.available_numbers AS Quantity
    FROM
        genre g
    JOIN
        book b ON g.genre_id = b.genre_id
    WHERE
        b.available_numbers < 5
)

SELECT
    Genre,
    Book,
    Quantity
FROM
    GenreBooks
ORDER BY
    Genre ASC,
    BookOrder ASC;
''', con)
print(task5)
print('\n')

con.close()