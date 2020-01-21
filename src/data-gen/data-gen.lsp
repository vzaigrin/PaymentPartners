;;;; Генератор данных по шаблону
(ql:quickload '("cl-json"))

(defun usage (&optional (e nil))
    (if e (format t "~%~a~%" e))
    (format t "~%Вызов: data-gen <-t | --template> filename") 
    (format t "~%                <-o | --output> filename") 
    (format t "~%                <-y | --year> number") 
    (format t "~%                <-m | --month> number") 
    (format t "~%                [<-n | --number-lines> number]") 
    (format t "~%                [<-e | --errors> number]") 
    (format t "~%                [<-v | --verbose>]") 
    (format t "~%                [<-h | --help>]") 
    (format t "~% где:") 
    (format t "~%  - template - файл с шаблоном") 
    (format t "~%  - output - файл со сгенерированными данными") 
    (format t "~%  - year - год, для генерации дат") 
    (format t "~%  - month - месяц, для генерации дат") 
    (format t "~%  - number-lines - кол-во генерируемых записей, по умолчанию - 100000") 
    (format t "~%  - errors - процент записей с ошибками, по умолчанию - 0") 
    (format t "~%  - verbose - флаг вывода информации о работе") 
    (format t "~%  - help - вывод этой информации~%~%") 
    (quit))

(defun exit-error (&optional (e nil))
    "Вывод сообщения об ошибке и завершение работы."
    (if e (format t "Ошибка: ~a~%~%" e))
    (quit))

;;; Структура для параметров
(defstruct parameters
    (templ-file "" :type string)
    (data-file "" :type string)
    (year 0 :type integer)
    (month 0 :type integer)
    (num-lines 100000 :type integer)
    (errors 0 :type integer)
    (verbose nil))

(defun string-integer-p (str)
    "Проверка, что строка может быть конвертирована в целое число"
    (every #'digit-char-p str))

(defun parse-args (args params)
    "Разбор аргументов коммандной строки. Возвращает структуру с параметрами"
    (cond
        ((null args) params)
        ((or (equal (car args) "-h") (equal (car args) "--help"))
            (usage))
        ((or (equal (car args) "-v") (equal (car args) "--verbose"))
            (setf (parameters-verbose params) t)
            (parse-args (cdr args) params))
        ((or (equal (car args) "-t") (equal (car args) "--template"))
            (if (cdr args)
                (progn
                    (setf (parameters-templ-file params) (cadr args))
                    (parse-args (cddr args) params))
                (usage "Не задано имя файла для шаблона")))
        ((or (equal (car args) "-o") (equal (car args) "--output"))
            (if (cdr args)
                (progn
                    (setf (parameters-data-file params) (cadr args))
                    (parse-args (cddr args) params))
                (usage "Не задано имя файла для данных")))
        ((or (equal (car args) "-y") (equal (car args) "--year"))
            (if (cdr args)
                (if (string-integer-p (cadr args))
                    (progn
                        (setf (parameters-year params) (parse-integer (cadr args)))
                        (parse-args (cddr args) params))
                    (usage
                        (format nil "Неправильно задан год: ~s должно быть числом" (cadr args))))
                (usage "Не задан год")))
        ((or (equal (car args) "-m") (equal (car args) "--month"))
            (if (cdr args)
                (if (string-integer-p (cadr args))
                    (progn
                        (setf (parameters-month params) (parse-integer (cadr args)))
                        (parse-args (cddr args) params))
                    (usage
                        (format nil "Неправильно задан месяц: ~s должно быть числом" (cadr args))))
                (usage "Не задан месяц")))
        ((or (equal (car args) "-n") (equal (car args) "--number-lines"))
            (if (cdr args)
                (if (string-integer-p (cadr args))
                    (progn
                        (setf (parameters-num-lines params) (parse-integer (cadr args)))
                        (parse-args (cddr args) params))
                    (usage
                        (format nil "Неправильно задано количество данных: ~s должно быть числом" (cadr args))))
                (usage "Не задано количество данных")))
        ((or (equal (car args) "-e") (equal (car args) "--errors"))
            (if (cdr args)
                (if (string-integer-p (cadr args))
                    (progn
                        (setf (parameters-errors params) (parse-integer (cadr args)))
                        (parse-args (cddr args) params))
                    (usage
                        (format nil "Неправильно задан процент некорректных данных: ~s должно быть числом" (cadr args))))
                (usage "Не задан процент некорректных данных")))
        (t (parse-args (cdr args) params))))

(defun decode-file (path)
    "Декодирование JSON файла"
    (let ((result 
            (handler-case
                (with-open-file (stream path) (json:decode-json stream))
                (json:json-syntax-error ()
                    (exit-error (format nil "JSON Syntax Error при чтении файла шаблона ~s" path))))))
        (handler-case (check-type result sequence)
            (SIMPLE-TYPE-ERROR () (exit-error (format nil "Неправильный формат файла шаблона ~s" path))))
        result))

;;; Структура для описания полей данных
(defstruct field
    (name "" :type string)
    (type "" :type string)
    (length nil)
    (nullable nil)
    (values))

(defun fillfield (lst)
    "Заполняем структуру поля данных по списку свойств"
    (let ((field (make-field)))
        (dolist (l lst)
            (cond
                ((eq (car l) :NAME) (setf (field-name field) (cdr l)))
                ((eq (car l) :TYPE) (setf (field-type field) (cdr l)))
                ((eq (car l) :LENGTH) (setf (field-length field) (cdr l)))
                ((eq (car l) :NULLABLE) (setf (field-nullable field) t))
                ((eq (car l) :VALUES) (setf (field-values field) (cadr l)))))
        field))

(defun list2string (lst &optional (sep ",") (frs nil))
    "Конвертируем список в строку с разделителем (по умолчанию - ',') между элементами"
    (cond
        ((null lst) "")
        (t (concatenate 'string
            (if frs sep "")
            (cond
                ((numberp (car lst)) (write-to-string (car lst)))
                (t (string (car lst))))
            (list2string (cdr lst) sep t)))))

(defun leap-year-p (year)
   "Определяем високосный год или нет"
    (and (zerop (mod year 4))
        (or (zerop (mod year 400))
            (not (zerop (mod year 100))))))

(defun days-in-month (year month)
    "Возвращаем количество дней в месяце по его номеру и году"
    (case month
        ((1 3 5 7 8 10 12) 31)
        ((4 6 9 11) 30)
        (2 (if (leap-year-p year) 29 28))))

(defun gen-timestamp (year month error)
    "Генерируем timestamp по году и месяцу с error процентом ошибок"
    (concatenate 'string
        (if (<= error (random 101))
            (format nil "~4,'0d" year)
            (format nil "~4,'0d" (1+ year)))
        "-"
        (format nil "~2,'0d" month) "-"
        (format nil "~2,'0d" (1+ (random (days-in-month year month))))
        " "
        (format nil "~2,'0d" (random 24)) ":"
        (format nil "~2,'0d" (random 60)) ":"
        (format nil "~2,'0d" (random 60))))

(defun get-from-list (lst)
    "Выбираем элемент из списка случайным образом"
    (nth (random (length lst)) lst))

(defun random-range (from to)
    "Генерируем случайное число в заданном интервале"
    (+ from (random (1+ to))))

(defun get-file (filename)
    "Читаем файл, возвращаем список"
    (with-open-file (stream filename)
        (loop for line = (read-line stream nil)
            while line
            collect line)))

(defun field2value (field year month error)
    "Генерируем значение по описанию поля"
    (cond
        ((field-nullable field) "")
        ((equal (field-type field) "TIMESTAMP")
            (gen-timestamp year month error))
        ((null (field-values field))
            (exit-error
                (format nil "Неправильный формат шаблона. Нет значения для поля ~a" (field-name field))))
        ((eq (car (field-values field)) :LIST)
            (get-from-list (cdr (field-values field))))
        ((eq (car (field-values field)) :RANGE)
            (cond
                ((equal (field-type field) "INT64")
                    (format nil
                        (if (field-length field)
                            (concatenate 'string "~" (write-to-string (field-length field)) ",'0d")
                            "~d")
                        (random-range
                            (second (field-values field))
                            (third (field-values field)))))
                ((equal (field-type field) "FLOAT64")
                    (format nil "~,2f"
                        (/ (random-range
                            (* 100 (second (field-values field)))
                            (* 100 (third (field-values field))))
                            100.0)))
                (t "")))
        (t "")))

(defun main ()
    ;; разбираем аргументы коммандной строки и заполняем структуру params
    (let ((params (parse-args (cdr sb-ext:*posix-argv*) (make-parameters)))
          (fields)) ; список разобранных полей данных
        ;; проверяем корректность заданных параметров
        (if (not (probe-file (parameters-templ-file params)))
            (exit-error (format nil "Файл для шаблона ~s не найден"
                (parameters-templ-file params))))
        (if (eq (parameters-year params) 0) (usage "Не задан год"))
        (if (eq (parameters-month params) 0) (usage "Не задан месяц"))
        (if (or
                (< (parameters-month params) 1)
                (> (parameters-month params) 12))
            (usage "Месяц должен быть от 1 до 12"))
        (if (or
                (< (parameters-errors params) 0)
                (> (parameters-errors params) 100))
            (usage "Процент некорректных данных должен быть от 0 до 100"))
        ;; Выводим заданные параметры
        (if (parameters-verbose params)
            (progn
                (format t "~%Файл с шаблоном: ~s" (parameters-templ-file params))
                (format t "~%Файл для данных: ~s"  (parameters-data-file params))
                (format t "~%Год: ~s"  (parameters-year params))
                (format t "~%Месяц: ~s"  (parameters-month params))
                (format t "~%Количество данных: ~s"  (parameters-num-lines params))
                (format t "~%Процент некорректных данных: ~s~%~%" (parameters-errors params))))
        ;; Декодируем шаблон в переменную fields
        ;; Из файла с шаблоном берём первое поле с именем "fields"
        (setf fields
            (mapcar #'fillfield
                (cdar (remove-if-not #'(lambda (x) (eq (car x) :FIELDS))
                    (decode-file (parameters-templ-file params))))))
        ;; Выходим, если поля не содержат имени и типа
        (if (some #'(lambda (x) (eq (field-name x) "")) fields)
            (exit-error "Неправильный формат шаблона. Нет имени поля"))
        (if (some #'(lambda (x) (eq (field-type x) "")) fields)
            (exit-error "Неправильный формат шаблона. Нет типа поля"))
        ;; Если поле содержит имя файла со списком, заменяем его на список из файла
        (setf fields
            (mapcar #'(lambda (x)
                (if (eq (car (field-values x)) :LIST-FILE)
                    (make-field
                        :name (field-name x)
                        :type (field-type x)
                        :length (field-length x)
                        :nullable (field-nullable x)
                        :values
                            (cons :LIST
                                (get-file
                                    (concatenate 'string
                                        (directory-namestring
                                            (truename (parameters-templ-file params)))
                                        (cdr (field-values x))))))
                    x))
                fields))
        ;; Выводим результат в файл
        (with-open-file
            (f (parameters-data-file params)
                :direction :output
                :if-exists :supersede
                :if-does-not-exist :create)
            ;; Выводим заголовок
            (format f "~a~%" (list2string (mapcar #'field-name fields)))
            ;; Выводим данные построчно
            (dotimes (i (parameters-num-lines params))
                (format f "~a~%"
                    (list2string (mapcar #'(lambda (x)
                        (field2value x
                            (parameters-year params)
                            (parameters-month params)
                            (parameters-errors params)))
                        fields)))))))

;;; Компилируем и выходим
(sb-ext:save-lisp-and-die "data-gen" :toplevel #'main :executable t)
