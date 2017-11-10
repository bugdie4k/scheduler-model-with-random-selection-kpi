(defpackage #:sched
  (:use #:cl))

(in-package #:sched)

(defstruct task
  (id nil)

  (arrival-time nil)

  (time-needed nil)
  (time-left nil)

  (waiting-time nil)
  (start-time nil)
  (end-time nil))

(defstruct scheduler-context
  (arrival->tasks-ht nil)
  (cur-time nil)
  (cur-task nil)
  (queue nil)
  (processed-tasks nil))

(defvar *max-time-needed* 10)

(defun gen-tasks (tasks-num intensity-modifier)
  (let ((max-arrival-time (floor (/ (* tasks-num *max-time-needed*)
                                    intensity-modifier))))
    (labels ((%gen-tasks (tasks-num id)
               (if (= 0 tasks-num)
                   nil
                   (cons (let ((time-needed (1+ (random *max-time-needed*)))
                               (arrival-time (1+ (random max-arrival-time))))
                           (make-task :id id
                                      :arrival-time arrival-time
                                      :time-needed time-needed
                                      :time-left time-needed
                                      :waiting-time 0))
                         (%gen-tasks (1- tasks-num) (1+ id))))))
      (cons (let ((time-needed (1+ (random *max-time-needed*))))
              (make-task :id 0
                         :arrival-time 0
                         :time-needed time-needed
                         :time-left time-needed
                         :waiting-time 0))
            (%gen-tasks (1- tasks-num) 1)))))

(defun mk-ht (tasks fn)
  (let ((ht (make-hash-table)))
    (dolist (task tasks)
      (let ((task-list (gethash (funcall fn task) ht)))
        (setf (gethash (funcall fn task) ht)
              (if (null task-list)
                  (list task)
                  (cons task task-list)))))
    ht))

(defun remove-nth (lst n)
  (labels ((%remove-nth (lst i)
             (when lst
               (if (= i n)
                   (%remove-nth (cdr lst) (1+ i))
                   (cons (car lst) (%remove-nth (cdr lst) (1+ i)))))))
    (%remove-nth lst 0)))

(defun select-new (queue)
  (let* ((random-idx (random (length queue)))
         (new-task (nth random-idx queue)))
    (values new-task (remove-nth queue random-idx))))

(defun iter (ctx)
  (let ((write-ctx ctx)
        (downtime 0))
    (with-slots
          (arrival->tasks-ht cur-time cur-task queue processed-tasks)
        write-ctx
      (labels ((%append-new-tasks-to-queue ()
                 (let ((new-tasks-list (gethash cur-time arrival->tasks-ht)))
                   (when new-tasks-list
                     (setf queue (append queue new-tasks-list))
                     (remhash cur-time arrival->tasks-ht))))
               (%select-new/upd-queue/upd-cur-task ()
                 (if queue
                     (multiple-value-bind (new-task new-queue)
                         (select-new queue)
                       (setf queue new-queue)
                       (setf cur-task new-task)
                       (setf (task-start-time cur-task) cur-time))
                     (incf downtime)))
               (%update-waiting-times ()
                 (let ((new-ht (make-hash-table)))
                   (maphash (lambda (k v)
                              (setf (gethash k new-ht)
                                    (mapcar
                                     (lambda (tsk)
                                       (setf (task-waiting-time tsk)
                                             (1+ (task-waiting-time tsk)))
                                       tsk)
                                     v)))
                            arrival->tasks-ht)))
               (%move-cur-to-processed ()
                 (let ((cur-task-wt-end-time cur-task))
                   (setf (task-end-time cur-task-wt-end-time) (1- cur-time))
                   (setf processed-tasks
                         (cons cur-task-wt-end-time processed-tasks))
                   (setf (scheduler-context-cur-task ctx) nil))))

        (%append-new-tasks-to-queue)

        (if (not cur-task)
            (%select-new/upd-queue/upd-cur-task)
            (progn
              (if (= (task-time-left cur-task) 0)
                  (progn (%move-cur-to-processed)
                         (%select-new/upd-queue/upd-cur-task))
                  (decf (task-time-left cur-task)))))

        (incf cur-time)
        (%update-waiting-times)
        (values write-ctx downtime)))))

(defun run (ctx &optional (downtime-arg 0))
  (multiple-value-bind (new-ctx downtime) (iter ctx)
    (if (and (= (hash-table-count
                 (scheduler-context-arrival->tasks-ht new-ctx))
                0)
             (null (scheduler-context-queue new-ctx))
             (null (scheduler-context-cur-task new-ctx)))
        (values new-ctx downtime-arg)
        (run new-ctx (+ downtime-arg downtime)))))

(defun print-graph (processed tasks-num)
  (let ((indent 0)
        (tasks-processed 0)
        (cur-time 0)
        (ht (mk-ht processed #'task-start-time)))
    (loop do
      (let ((tsk (car (gethash cur-time ht))))
        (if tsk
            (progn
              (loop for i from (task-start-time tsk) to (task-end-time tsk)
                    do (format
                        t "~3A|~A~A~%"
                        cur-time
                        (if (or (= cur-time (task-start-time tsk))
                                (= cur-time (task-end-time tsk)))
                            (make-string (1+ indent) :initial-element #\-)
                            (make-string (1+ indent) :initial-element #\space))
                        (task-id tsk))
                       (incf cur-time))
              (incf tasks-processed))
            (if (= tasks-processed tasks-num)
                (return)
                (progn (format t "~3A|~%" cur-time)
                       (incf cur-time))))
        (incf indent)))))

(defun print-task-report (task)
  (with-slots (id time-needed arrival-time waiting-time start-time end-time)
      task
    (format t "| ~5A | ~5A | ~5A | ~5A | ~5A | ~5A |~%"
            id time-needed arrival-time waiting-time start-time end-time)))

(defun print-report (processed tasks-num)
  (format t "~A~A~A~%"
          (make-string 20 :initial-element #\-)
          " REPORT "
          (make-string 21 :initial-element #\-))
  (format t "| ~5A | ~5A | ~5A | ~5A | ~5A | ~5A |~%"
          "ID" "NEED" "ARRI" "WAIT" "START" "END")
  (format t "~A~%" (make-string 49 :initial-element #\-))
  ;; ---
  (let ((ht (mk-ht processed #'task-id)))
    (dotimes (i tasks-num)
      ;; (print i)
      ;; (print (car (gethash i ht)))
      (print-task-report (car (gethash i ht)))))
  ;; ---
  (format t "~A~%" (make-string 49 :initial-element #\-)))

(defun print-graph-report (processed tasks-num)
  (print-graph processed tasks-num)
  (print-report processed tasks-num))

(defun test (&key (num 10) (report nil) (intensity-modifier 3))
  (multiple-value-bind (new-scheduler-ctx downtime)
      (run
       (make-scheduler-context
        :arrival->tasks-ht
        (mk-ht (gen-tasks num intensity-modifier) #'task-arrival-time)
        :cur-time 0))
    (let* ((processed (scheduler-context-processed-tasks new-scheduler-ctx))
           (observation-time (task-end-time (car processed)))
           (intensity (/ num observation-time))
           (mid-wait-time
             (/ (reduce (lambda (acc tsk)
                          (+ acc (task-waiting-time tsk)))
                        processed
                        :initial-value 0)
                observation-time)))
      (when report (print-graph-report processed num))
      (format t "~12,8F ~12,8F ~3D~%" intensity mid-wait-time downtime)
      (values intensity mid-wait-time downtime))))

(defun tests (&key (times 100) (num 200) (intensity-modifier 3))
  (loop for i from 1 to times do
    (test :num num :intensity-modifier intensity-modifier)))
