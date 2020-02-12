;;;; Генератор DDL по шаблону
(ql:quickload '("cl-json"))

(defun usage (&optional (e nil))
    (if e (format t "~%~a~%" e))
    (format t "~%Вызов: ddl-gen <-p | --partner partner-name")
    (format t "~%               <-t | --template> filename") 
    (format t "~%               <-o | --output> filename") 
    (format t "~%              [<-v | --verbose>]") 
    (format t "~%              [<-h | --help>]") 
    (format t "~% где:") 
    (format t "~%  - partner-name - название партнёра")
    (format t "~%  - template - файл с шаблоном")
    (format t "~%  - output - файл со сгенерированными DDL")
    (format t "~%  - verbose - флаг вывода информации о работе")
    (format t "~%  - help - вывод этой информации~%~%")
    (quit))

(defun exit-error (&optional (e nil))
    "Вывод сообщения об ошибке и завершение работы."
    (if e (format t "~%Ошибка: ~a~%~%" e))
    (quit))

(defconstant +project+ "my-project-1530001957977")  ; GCP Project ID
(defconstant +ds+ "PP")                             ; BigQuery dataset
(defconstant +tab+ (string #\tab))
(defconstant +nl+ (string #\nl))
(defconstant +targets+
    (list "bin" "card_number" "operation_ts" "operation_country" "operation_city"
          "payment_total" "payment_tariff" "payment_main_client" "payment_ps"
          "payment_partner" "payment_other_client"))

;;; Структура для параметров
(defstruct parameters
    (partner)
    (template-file)
    (ddl-file)
    (verbose nil))

(defun parse-args (args params)
    "Разбор аргументов коммандной строки. Возвращает структуру с параметрами"
    (cond
        ((null args) params)
        ((or (equal (car args) "-h") (equal (car args) "--help"))
            (usage))
        ((or (equal (car args) "-v") (equal (car args) "--verbose"))
            (setf (parameters-verbose params) t)
            (parse-args (cdr args) params))
        ((or (equal (car args) "-p") (equal (car args) "--partner"))
            (if (cdr args)
                (progn
                    (setf (parameters-partner params) (cadr args))
                    (parse-args (cddr args) params))
                (usage "Не задано имя файла для шаблона")))
        ((or (equal (car args) "-t") (equal (car args) "--template"))
            (if (cdr args)
                (progn
                    (setf (parameters-template-file params) (cadr args))
                    (parse-args (cddr args) params))
                (usage "Не задано имя файла для шаблона")))
        ((or (equal (car args) "-o") (equal (car args) "--output"))
            (if (cdr args)
                (progn
                    (setf (parameters-ddl-file params) (cadr args))
                    (parse-args (cddr args) params))
                (usage "Не задано имя файла для данных")))
        (t (parse-args (cdr args) params))))

(defun decode-file (path)
    "Декодирование JSON файла"
    (let ((result 
            (handler-case
                (with-open-file (stream path) (json:decode-json stream))
                (json:json-syntax-error ()
                    (exit-error (format nil "JSON Syntax Error при чтении файла шаблона ~s" path))))))
        (if (typep result 'sequence)
            result
            (exit-error (format nil "Неправильный формат файла шаблона ~s" path)))))

(defun fields2list (fields)
    "Конвертируем список fields в список пар 'Имя Тип'"
    (mapcar #'(lambda (x) (cons (cdr (assoc :NAME x)) (cdr (assoc :TYPE x)))) fields))

(defun dds2hash (lst)
    "Конвертируем список полей DDS в хэш-таблицу"
    (let ((hash (make-hash-table :test #'equal)))
        (mapcar
            #'(lambda (x) (setf (gethash (cdr (assoc :TARGET x)) hash) (cdr (assoc :SOURCE x))))
            lst)
        hash))

(defun list2string (lst &optional (sep ",") (frs nil))
    "Конвертируем список строк в строку с разделителем (по умолчанию ',') между элементами"
    (cond
        ((null lst) "")
        (t (concatenate 'string
                (if frs sep "")
                (cond
                    ((numberp (car lst)) (write-to-string (car lst)))
                    ((atom (car lst)) (string (car lst))))
                (list2string (cdr lst) sep t)))))

(defun null-value (f-type)
    "Возвращаем представление NULL по типу"
    (cond
        ((equal (string-upcase f-type) "STRING") "''")
        ((equal (string-upcase f-type) "DATE") "DATE(pyear, pmonth, 1)")
        ((equal (string-upcase f-type) "TIME") "TIME(0, 0, 0)")
        ((equal (string-upcase f-type) "TIMESTAMP") "TIMESTAMP(DATE(pyear, pmonth, 1))")
        ((equal (string-upcase f-type) "INT64") "0")
        ((equal (string-upcase f-type) "FLOAT64") "0.0")
        (t "")))

(defun create-table (table-type partner fields-list)
    "Возвращаем строку для создания таблицы"
    (let*
        ((max-name (apply #'max (mapcar #'length (mapcar #'car fields-list))))
         (max-format (format nil "~A~A~A" "~" max-name "A~A~A"))
         (max-format2 (format nil "~A~A~A" "~" (+ 2 max-name) "A~A~A")))
        (concatenate 'string
            "DROP TABLE IF EXISTS " +ds+ "." table-type "_" (string-upcase partner) ";" +nl+
            "CREATE TABLE "+ds+ "." (string-upcase table-type) "_" (string-upcase partner) " (" +nl+
            +tab+ (format nil max-format2 (caar fields-list) +tab+ (cdar fields-list)) +nl+
            +tab+ ", "
            (list2string
                (mapcar #'(lambda (x) (format nil max-format (car x) +tab+ (cdr x))) (cdr fields-list))
                (concatenate 'string +nl+ +tab+ ", ")) +nl+
            (if (equal (string-upcase table-type) "ODS")
                (concatenate 'string
                    +tab+ (format nil max-format2 ", period_year" +tab+ "INT64") +nl+
                    +tab+ (format nil max-format2 ", period_month" +tab+ "INT64") +nl+
                    +tab+ (format nil max-format2 ", filename" +tab+ "STRING") +nl+
                    +tab+ (format nil max-format2 ", load_ts" +tab+ "TIMESTAMP") +nl+))
            ");" +nl+)))

(defun create-view (partner)
    "Возвращаем строку для создания представлений"
    (concatenate 'string
        "CREATE OR REPLACE VIEW `" +project+ "." (string-upcase partner) ".V_DM_LOADS` AS" +nl+
        "SELECT" +nl+
        +tab+ "period_name" +nl+
        +tab+ ", period_year" +nl+
        +tab+ ", period_month" +nl+
        +tab+ ", filename" +nL+
        +tab+ ", load_ts" +nl+
        +tab+ ", stg" +nl+
        +tab+ ", ods" +nl+
        +tab+ ", dds" +nl+
        +tab+ ", bad" +nl+
        "FROM `" +project+ "." +ds+ ".DM_LOADS` p" +nl+
        "WHERE upper(p.partner_name) = '" (string-upcase partner) "'" +nl+
        ";" +nl+
        +nl+
        "CREATE OR REPLACE VIEW `" +project+ "." (string-upcase partner) ".V_DM_BANK` AS" +nl+
        "SELECT" +nl+
        +tab+ "sum_opers" +nl+
        +tab+ ", avg_total" +nl+
        +tab+ ", bank" +nl+
        +tab+ ", operation_day" +nl+
        +tab+ ", period_name" +nl+
        +tab+ ", period_year" +nl+
        +tab+ ", period_month" +nl+
        +tab+ ", week_num" +nl+
        "FROM `" +project+ "." +ds+ ".V_DM_BANK` p" +nl+
        "WHERE upper(p.partner_name) = '" (string-upcase partner) "'" +nl+
        ";" +nl+
        +nl+
        "CREATE OR REPLACE VIEW `" +project+ "." (string-upcase partner) ".V_DM_CARD` AS" +nl+
        "SELECT" +nl+
        +tab+ "sum_opers" +nl+
        +tab+ ", avg_total" +nl+
        +tab+ ", card_type" +nl+
        +tab+ ", operation_day" +nl+
        +tab+ ", period_name" +nl+
        +tab+ ", period_year" +nl+
        +tab+ ", period_month" +nl+
        +tab+ ", week_num" +nl+
        "FROM `" +project+ "." +ds+ ".V_DM_CARD` p" +nl+
        "WHERE upper(p.partner_name) = '" (string-upcase partner) "'" +nl+
        ";" +nl+
        +nl+
        "CREATE OR REPLACE VIEW `" +project+ "." (string-upcase partner) ".V_DM_CITY` AS" +nl+
        "SELECT" +nl+
        +tab+ "sum_opers" +nl+
        +tab+ ", avg_total" +nl+
        +tab+ ", operation_city" +nl+
        +tab+ ", operation_day" +nl+
        +tab+ ", period_name" +nl+
        +tab+ ", period_year" +nl+
        +tab+ ", period_month" +nl+
        +tab+ ", week_num" +nl+
        "FROM `" +project+ "." +ds+ ".V_DM_CITY` p" +nl+
        "WHERE upper(p.partner_name) = '" (string-upcase partner) "'" +nl+
        ";" +nl+
        +nl+
        "CREATE OR REPLACE VIEW `" +project+ "." (string-upcase partner) ".V_DM_COUNTRY` AS" +nl+
        "SELECT" +nl+
        +tab+ "sum_opers" +nl+
        +tab+ ", avg_total" +nl+
        +tab+ ", operation_country" +nl+
        +tab+ ", operation_day" +nl+
        +tab+ ", period_name" +nl+
        +tab+ ", period_year" +nl+
        +tab+ ", period_month" +nl+
        +tab+ ", week_num" +nl+
        "FROM `" +project+ "." +ds+ ".V_DM_COUNTRY` p" +nl+
        "WHERE upper(p.partner_name) = '" (string-upcase partner) "'" +nl+
        ";" +nl+
        +nl+
        "CREATE OR REPLACE VIEW `" +project+ "." (string-upcase partner) ".V_DM_PRIVILEGE` AS" +nl+
        "SELECT" +nl+
        +tab+ "sum_opers" +nl+
        +tab+ ", privilege_type" +nl+
        +tab+ ", operation_day" +nl+
        +tab+ ", period_name" +nl+
        +tab+ ", period_year" +nl+
        +tab+ ", period_month" +nl+
        +tab+ ", week_num" +nl+
        "FROM `" +project+ "." +ds+ ".V_DM_PRIVILEGE` p" +nl+
        "WHERE upper(p.partner_name) = '" (string-upcase partner) "'" +nl+
        ";" +nl+
        +nl+
        "CREATE OR REPLACE VIEW `" +project+ "." (string-upcase partner) ".V_DM_PAYMENT` AS" +nl+
        "SELECT" +nl+
        +tab+ "avg_client" +nl+
        +tab+ ", avg_ps" +nl+
        +tab+ ", avg_partner" +nl+
        +tab+ ", operation_day" +nl+
        +tab+ ", period_name" +nl+
        +tab+ ", period_year" +nl+
        +tab+ ", period_month" +nl+
        +tab+ ", week_num" +nl+
        "FROM `" +project+ "." +ds+ ".V_DM_PAYMENT` p" +nl+
        "WHERE upper(p.partner_name) = '" (string-upcase partner) "'" +nl+
        ";" +nl+))

(defun create-load (partner fields-list dds-hash)
    "Возвращаем строку для создания процедуры загрузки таблиц"
    (let ((ots (find (gethash "operation_ts" dds-hash) fields-list :key #'car :test #'equal)))
        (concatenate 'string
            "CREATE OR REPLACE PROCEDURE " +ds+ ".LOAD_" (string-upcase partner) " (fname STRING, pyear INT64, pmonth INT64)" +nl+
            "BEGIN" +nl+
            +nl+
            +tab+ "DECLARE loadts TIMESTAMP;" +nl+
            +tab+ "DECLARE stg INT64;" +nl+
            +tab+ "DECLARE ods INT64;" +nl+
            +tab+ "DECLARE dds INT64;" +nl+
            +nl+
            +tab+ "SET loadts = CURRENT_TIMESTAMP;" +nl+
            +tab+ "SET stg = (SELECT count(*) FROM " +ds+ ".STG_" (string-upcase partner) ");" +nl+
            +nl+
            +tab+ "DELETE FROM " +ds+ ".ODS_" (string-upcase partner) " WHERE true;" +nl+
            +nl+
            +tab+ "INSERT INTO " +ds+ ".ODS_" (string-upcase partner) +nl+
            +tab+ "SELECT DISTINCT"
            (format nil "~%~A~A~A" #\tab #\tab
                (list2string
                    (mapcar #'(lambda (x) (format nil "COALESCE(~A, ~A)" (car x) (null-value (cdr x)))) fields-list)
                    (concatenate 'string +nl+ +tab+ +tab+ ", "))) +nl+
            +tab+ +tab+ ", pyear AS period_year" +nl+
            +tab+ +tab+ ", pmonth AS period_month" +nl+
            +tab+ +tab+ ", fname AS filename" +nl+
            +tab+ +tab+ ", loadts AS load_ts" +nl+
            +tab+ "FROM " +ds+ ".STG_" (string-upcase partner) +nl+
            +tab+ "WHERE "
                (if (equal (cdr ots) "TIMESTAMP")
                    (format nil "DATE(~A)" (car ots))
                    (car ots))
                " BETWEEN DATE(pyear, pmonth, 1) AND DATE_ADD(DATE(pyear, pmonth, 1), INTERVAL 1 MONTH)" +nl+
            +tab+ ";" +nl+
            +tab+ +nl+
            +tab+ "SET ods = (SELECT count(*) FROM " +ds+ ".ODS_" (string-upcase partner) ");" +nl+
            +nl+
            +tab+ "DELETE FROM " +ds+ ".TMP_DATA WHERE true;" +nl+
            +nL+
            +tab+ "INSERT INTO " +ds+ ".TMP_DATA" +nl+
            +tab+ "SELECT" +nl+
            +tab+ +tab+ (gethash "bin" dds-hash) " AS bin" +nl+
            +tab+ +tab+ ", " (gethash "card_number" dds-hash) " AS card_number" +nl+
            +tab+ +tab+ ", " (gethash "operation_ts" dds-hash) " AS operation_ts" +nl+
            +tab+ +tab+ ", pyear AS period_year" +nl+
            +tab+ +tab+ ", pmonth AS period_month" +nl+
            +tab+ +tab+ ", FORMAT(\"%4d-%02d\", pyear, pmonth) AS period_name" +nl+
            +tab+ +tab+ ", " (gethash "operation_country" dds-hash) " AS operation_country" +nl+
            +tab+ +tab+ ", " (gethash "operation_city" dds-hash) " AS operation_city" +nl+
            +tab+ +tab+ ", " (gethash "payment_total" dds-hash) " AS payment_total" +nl+
            +tab+ +tab+ ", " (gethash "payment_tariff" dds-hash) " AS payment_tariff" +nl+
            +tab+ +tab+ ", " (gethash "payment_main_client" dds-hash) " AS payment_main_client" +nl+
            +tab+ +tab+ ", " (gethash "payment_ps" dds-hash) " AS payment_ps" +nl+
            +tab+ +tab+ ", " (gethash "payment_partner" dds-hash) " AS payment_partner" +nl+
            +tab+ +tab+ ", " (gethash "payment_other_client" dds-hash) " AS payment_other_client" +nl+
            +tab+ +tab+ ", " (gethash "privilege_type" dds-hash) " AS privilege_type" +nl+
            +tab+ +tab+ ", load_ts AS processed_dttm" +nl+
            +tab+ "FROM " +ds+ ".ODS_" (string-upcase partner) +nl+
            +tab+ ";" +nl+
            +nl+
            +tab+ "CALL " +ds+ ".LOAD_DATA('" partner "', dds);" +nl+
            +nl+
            +tab+ "INSERT INTO " +ds+ ".DM_LOADS" +nl+
            +tab+ "VALUES ('" partner "', FORMAT(\"%4d-%02d\", pyear, pmonth), pyear, pmonth, fname, loadts, stg, ods, dds, stg - dds);" +nl+
            +nl+
            "END;" +nl+)))

(defun main ()
    ;; разбираем аргументы коммандной строки и заполняем структуру params
    (let ((params (parse-args (cdr sb-ext:*posix-argv*) (make-parameters)))
          (template)    ; содержимое файла с шаблоном
          (fields)      ; список разобранных полей данных
          (fields-list) ; список пар (Имя Тип)
          (dds)         ; список разобранных полей DDS
          (dds-hash))   ; хэш-таблица полей DDS
        ;; проверяем корректность заданных параметров
        (if (not (parameters-template-file params))
            (usage "Файл для шаблона не задан"))
        (if (not (probe-file (parameters-template-file params)))
            (usage (format nil "Файл для шаблона ~s не найден"
                (parameters-template-file params))))
        (if (not (parameters-ddl-file params))
            (usage "Файл для вывода DDL не задан"))
        (if (not (parameters-partner params))
            (usage "Название партнёра не задано"))
        ;; Выводим заданные параметры
        (if (parameters-verbose params)
            (format t "~%Партнёр: ~s~%Файл с шаблоном: ~s~%Файл для DDL: ~s~%"
                (parameters-partner params)
                (parameters-template-file params)
                (parameters-ddl-file params)))
        ;; Читаем файл с шаблоном
        (setf template (decode-file (parameters-template-file params)))
        ;; Декодируем поля данных шаблона в переменную fields
        ;; Из файла с шаблоном берём первое поле с именем "fields"
        ;; (setf fields (cdr (find :FIELDS template :key #'car :test #'equal)))
        (setf fields (cdr (assoc :FIELDS template)))
        ;; Выходим, если файл не содержит список fields
        (if (not fields) (exit-error "Не задан список Fields в файле с шаблоном"))
        ;; Выходим, если поля не содержат имени и типа
        (if (not (every #'(lambda (x) (assoc :NAME x)) fields))
            (exit-error "Неправильный шаблон. Fields: Нет имени поля"))
        (if (not (every #'(lambda (x) (assoc :TYPE x)) fields))
            (exit-error "Неправильный шаблон. Fields: Нет типа поля"))
        ;; Получаем список пар (Имя Тип)
        (setf fields-list (fields2list fields))
        ;; Декодируем поля DDS шаблона в переменную dds
        ;; Из файла с шаблоном берём первое поле с именем "dds"
        (setf dds (cdr (assoc :DDS template)))
        ;; Выходим, если файл не содержит список DDS
        (if (not dds) (exit-error "Не задана список DDS в файле с шиаблоном"))
        ;; Выходим, если поля не содержат имени и типа
        (if (not (every #'(lambda (x) (assoc :TARGET x)) dds))
            (exit-error "Неправильный шаблон. DDS: Нет Target"))
        (if (not (every #'(lambda (x) (assoc :SOURCE x)) dds))
            (exit-error "Неправильный шаблон. DDS: Нет Source"))
        ;; Конвертируем список полей DDS в хэш таблицу
        (setf dds-hash (dds2hash dds))
        ;; Выходим, если хэш-таблица содержит не все поля
        (dolist (x +targets+)
            (if (not (gethash x dds-hash))
                (exit-error (format nil "Неправильный шаблон. В DDS не опеределен: ~A" x))))
        ;; Выводим результат в файл
        (with-open-file
            (f (parameters-ddl-file params)
                :direction :output
                :if-exists :supersede
                :if-does-not-exist :create)
            (format f "~A" (create-table "STG" (parameters-partner params) fields-list))
            (terpri f)
            (format f "~A" (create-table "ODS" (parameters-partner params) fields-list))
            (terpri f)
            (format f "~A" (create-view (parameters-partner params)))
            (terpri f)
            (format f "~A" (create-load (parameters-partner params) fields-list dds-hash)))))

;;; Компилируем и выходим
(sb-ext:save-lisp-and-die "ddl-gen" :toplevel #'main :executable t)
