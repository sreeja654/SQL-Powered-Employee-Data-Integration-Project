create database sql_project;
use sql_project;

CREATE TABLE existing_employees (
    Employee_ID VARCHAR(25) PRIMARY KEY,
    Employee_Name varchar(50),
    Salary BIGINT,
    Project_ID VARCHAR(25),
    Project_Name varchar(50),
    Client_ID VARCHAR(25),
    Client_Name varchar(50),
    Project_Budget BIGINT,
    Employee_Hire_Date DATE,
    Project_Duration_in_Days BIGINT,
    Employee_Designation varchar(50),
    Employee_Skills varchar(50),
    Emp_Phone_No BIGINT,
    Office_Location varchar(50),
    We_Region varchar(50),
    Employee_Gender varchar(20)
);
SELECT * FROM existing_employees;
delete from existing_employees
where employee_id="E1033333";

CREATE TABLE existing_employees_permanent (
    Employee_ID VARCHAR(25) PRIMARY KEY,
    Employee_Name VARCHAR(50),
    Salary BIGINT,
    Project_ID VARCHAR(25),
    Project_Name VARCHAR(50),
    Client_ID VARCHAR(25),
    Client_Name VARCHAR(50),
    Project_Budget BIGINT,
    Employee_Hire_Date DATE,
    Project_Duration_in_Days BIGINT,
    Employee_Designation VARCHAR(50),
    Employee_Skills VARCHAR(50),
    Emp_Phone_No BIGINT,
    Office_Location VARCHAR(50),
    We_Region VARCHAR(50),
    Employee_Gender VARCHAR(20),
    indicator ENUM('yes', 'no') DEFAULT 'yes'
);


INSERT INTO existing_employees_permanent 
SELECT *, 'yes' AS indicator FROM existing_employees;

select * from existing_employees_permanent;

CREATE TABLE log_table (
    Employee_ID VARCHAR(25),
    Data_inserted ENUM('yes', 'no') DEFAULT 'no',
    Data_updated ENUM('yes', 'no') DEFAULT 'no',
    Data_deleted ENUM('yes', 'no') DEFAULT 'no',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

select * from log_table;


DELIMITER $$

DROP PROCEDURE IF EXISTS ManageEmployeeData$$

CREATE PROCEDURE ManageEmployeeData(
    IN p_Employee_ID VARCHAR(50),
    IN p_Employee_Name VARCHAR(100),
    IN p_Salary DECIMAL(10,2),
    IN p_Project_ID VARCHAR(50),
    IN p_Project_Name VARCHAR(100),
    IN p_Client_ID VARCHAR(50),
    IN p_Client_Name VARCHAR(100),
    IN p_Project_Budget DECIMAL(15,2),
    IN p_Employee_Hire_Date DATE,
    IN p_Project_Duration_in_Days INT,
    IN p_Employee_Designation VARCHAR(100),
    IN p_Employee_Skills TEXT,
    IN p_Emp_Phone_No VARCHAR(20),
    IN p_Office_Location VARCHAR(100),
    IN p_We_Region VARCHAR(50),
    IN p_Employee_Gender VARCHAR(10),
    IN p_Action VARCHAR(10) -- 'insert', 'update', or 'delete'
)
BEGIN
    IF p_Action = 'insert' THEN
        -- Insert into existing_employees_permanent
        INSERT INTO existing_employees_permanent (
            Employee_ID, Employee_Name, Salary, Project_ID, Project_Name, Client_ID, Client_Name, Project_Budget,
            Employee_Hire_Date, Project_Duration_in_Days, Employee_Designation, Employee_Skills, Emp_Phone_No,
            Office_Location, We_Region, Employee_Gender, indicator
        )
        VALUES (
            p_Employee_ID, p_Employee_Name, p_Salary, p_Project_ID, p_Project_Name, p_Client_ID, p_Client_Name, p_Project_Budget,
            p_Employee_Hire_Date, p_Project_Duration_in_Days, p_Employee_Designation, p_Employee_Skills, p_Emp_Phone_No,
            p_Office_Location, p_We_Region, p_Employee_Gender, 'yes'
        )
        ON DUPLICATE KEY UPDATE 
            Employee_Name = p_Employee_Name,
            Salary = p_Salary,
            Project_ID = p_Project_ID,
            Project_Name = p_Project_Name,
            Client_ID = p_Client_ID,
            Client_Name = p_Client_Name,
            Project_Budget = p_Project_Budget,
            Employee_Hire_Date = p_Employee_Hire_Date,
            Project_Duration_in_Days = p_Project_Duration_in_Days,
            Employee_Designation = p_Employee_Designation,
            Employee_Skills = p_Employee_Skills,
            Emp_Phone_No = p_Emp_Phone_No,
            Office_Location = p_Office_Location,
            We_Region = p_We_Region,
            Employee_Gender = p_Employee_Gender,
            indicator = 'yes';

        -- Insert log entry
        INSERT INTO log_table (Employee_ID, Data_inserted, Data_updated, Data_deleted)
        VALUES (p_Employee_ID, 'yes', 'no', 'no')
        ON DUPLICATE KEY UPDATE Data_inserted = 'yes', Data_updated = 'no', Data_deleted = 'no';

    ELSEIF p_Action = 'update' THEN
        -- Update existing_employees_permanent
        UPDATE existing_employees_permanent
        SET 
            Employee_Name = p_Employee_Name,
            Salary = p_Salary,
            Project_ID = p_Project_ID,
            Project_Name = p_Project_Name,
            Client_ID = p_Client_ID,
            Client_Name = p_Client_Name,
            Project_Budget = p_Project_Budget,
            Employee_Hire_Date = p_Employee_Hire_Date,
            Project_Duration_in_Days = p_Project_Duration_in_Days,
            Employee_Designation = p_Employee_Designation,
            Employee_Skills = p_Employee_Skills,
            Emp_Phone_No = p_Emp_Phone_No,
            Office_Location = p_Office_Location,
            We_Region = p_We_Region,
            Employee_Gender = p_Employee_Gender,
            indicator = 'yes'
        WHERE Employee_ID = p_Employee_ID;

        -- Update log_table
        INSERT INTO log_table (Employee_ID, Data_inserted, Data_updated, Data_deleted)
        VALUES (p_Employee_ID, 'no', 'yes', 'no')
        ON DUPLICATE KEY UPDATE Data_inserted = 'no', Data_updated = 'yes', Data_deleted = 'no';

    ELSEIF p_Action = 'delete' THEN
        -- Mark employee as deleted in existing_employees_permanent
        UPDATE existing_employees_permanent
        SET indicator = 'no'
        WHERE Employee_ID = p_Employee_ID;

        -- Update log_table
        INSERT INTO log_table (Employee_ID, Data_inserted, Data_updated, Data_deleted)
        VALUES (p_Employee_ID, 'no', 'no', 'yes')
        ON DUPLICATE KEY UPDATE Data_inserted = 'no', Data_updated = 'no', Data_deleted = 'yes';

    END IF;
END$$

DELIMITER ;
-- Create TRIGER FOR CALLING STORE PROCIDURE for automatically insert

DELIMITER $$

CREATE TRIGGER after_insert_existing_employees
AFTER INSERT ON existing_employees
FOR EACH ROW
BEGIN
    CALL ManageEmployeeData(
        NEW.Employee_ID, NEW.Employee_Name, NEW.Salary, NEW.Project_ID, NEW.Project_Name, NEW.Client_ID, 
        NEW.Client_Name, NEW.Project_Budget, NEW.Employee_Hire_Date, NEW.Project_Duration_in_Days, 
        NEW.Employee_Designation, NEW.Employee_Skills, NEW.Emp_Phone_No, NEW.Office_Location, NEW.We_Region, 
        NEW.Employee_Gender, 'insert'
    );
END$$

DELIMITER ;

-- Create TRIGER FOR CALLING STORE PROCIDURE for automatically UPDATE

DELIMITER $$

CREATE TRIGGER after_update_existing_employees
AFTER UPDATE ON existing_employees
FOR EACH ROW
BEGIN
    CALL ManageEmployeeData(
        NEW.Employee_ID, NEW.Employee_Name, NEW.Salary, NEW.Project_ID, NEW.Project_Name, NEW.Client_ID, 
        NEW.Client_Name, NEW.Project_Budget, NEW.Employee_Hire_Date, NEW.Project_Duration_in_Days, 
        NEW.Employee_Designation, NEW.Employee_Skills, NEW.Emp_Phone_No, NEW.Office_Location, NEW.We_Region, 
        NEW.Employee_Gender, 'update'
    );
END$$

DELIMITER ;


-- Create TRIGER FOR CALLING STORE PROCIDURE for automatically DELETE

DELIMITER $$

CREATE TRIGGER after_delete_existing_employees
AFTER DELETE ON existing_employees
FOR EACH ROW
BEGIN
    CALL ManageEmployeeData(
        OLD.Employee_ID, OLD.Employee_Name, OLD.Salary, OLD.Project_ID, OLD.Project_Name, OLD.Client_ID, 
        OLD.Client_Name, OLD.Project_Budget, OLD.Employee_Hire_Date, OLD.Project_Duration_in_Days, 
        OLD.Employee_Designation, OLD.Employee_Skills, OLD.Emp_Phone_No, OLD.Office_Location, OLD.We_Region, 
        OLD.Employee_Gender, 'delete'
    );
END$$

DELIMITER ;

-- CREATE JOB_APPLICATIONS TABLE
CREATE TABLE job_applications (
    Applicant_Name VARCHAR(100),
    Experience_of_Applicant INT,
    Skills VARCHAR(255),
    Job_Application_ID VARCHAR(20),
    PRIMARY KEY (Applicant_Name, Job_Application_ID)
);

SELECT * FROM job_applications;


CREATE TABLE open_positions (
    Project_Name VARCHAR(100),
    Open_Positions VARCHAR(100),
    Key_Skills VARCHAR(255),
    Job_Application_ID VARCHAR(20) PRIMARY KEY
);

SELECT * FROM open_positions;
delete FROM open_positions
WHERE KEY_SKILLS ="K";
SET SQL_SAFE_UPDATES = 0;

#--SEGREGATING
CREATE TABLE IF NOT EXISTS job_applications_skills (
    Applicant_Name VARCHAR(50),
    Experience_of_Applicant int,
    Skill VARCHAR(50),
    Job_Application_ID VARCHAR(20),
    PRIMARY KEY (Applicant_Name, Skill, Job_Application_ID),
    FOREIGN KEY (Applicant_Name, Job_Application_ID) 
        REFERENCES job_applications(Applicant_Name, Job_Application_ID) 
        ON DELETE CASCADE
);
select * from job_applications;
select * from job_applications_skills;
#NOW WE INSERT DATA,CREATING DELIMITER FOR CREATING STORED PROCEDURE


DELIMITER $$

CREATE PROCEDURE InsertIntoJobApplicationsSkills()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_Applicant_Name VARCHAR(50);
    DECLARE v_Experience_of_Applicant INT;
    DECLARE v_Skills VARCHAR(150);
    DECLARE v_Job_Application_ID VARCHAR(20);
    DECLARE skill VARCHAR(50);
    DECLARE skill_cursor CURSOR FOR 
        SELECT Applicant_Name, Experience_of_Applicant, Skills, Job_Application_ID 
        FROM job_applications;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN skill_cursor;

    read_loop: LOOP
        FETCH skill_cursor INTO v_Applicant_Name, v_Experience_of_Applicant, v_Skills, v_Job_Application_ID;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Split skills and insert each one separately
        WHILE LENGTH(v_Skills) > 0 DO
            SET skill = SUBSTRING_INDEX(v_Skills, ',', 1);  -- Extract the first skill
            SET v_Skills = IF(LOCATE(',', v_Skills) > 0, SUBSTRING(v_Skills, LOCATE(',', v_Skills) + 1), '');  -- Remove the extracted skill

            -- Insert the record
            INSERT INTO job_applications_skills (Applicant_Name, Experience_of_Applicant, Skill, Job_Application_ID)
            VALUES (v_Applicant_Name, v_Experience_of_Applicant, skill, v_Job_Application_ID);
        END WHILE;
    END LOOP;

    CLOSE skill_cursor;
END$$

DELIMITER ;
#MANUALLY CALLING THE STORED PROCEDURE OR DELIMITER
CALL InsertIntoJobApplicationsSkills();
#FOR AUTOMATION FOR CALLING STORED PROCEDURE WE USE TRIGGER
DELIMITER $$

CREATE TRIGGER trg_InsertJobApplicationsSkills
AFTER INSERT ON job_applications
FOR EACH ROW
BEGIN
    DECLARE skill VARCHAR(50);
    DECLARE remaining_skills VARCHAR(150);

    SET remaining_skills = NEW.Skills;  -- Get the skills from the inserted row

    -- Loop through each skill in the comma-separated list
    WHILE LENGTH(remaining_skills) > 0 DO
        SET skill = SUBSTRING_INDEX(remaining_skills, ',', 1);  -- Extract first skill
        SET remaining_skills = IF(LOCATE(',', remaining_skills) > 0, 
                                  SUBSTRING(remaining_skills, LOCATE(',', remaining_skills) + 1), 
                                  '');  -- Remove the extracted skill

        -- Insert each skill separately into job_applications_skills
        INSERT INTO job_applications_skills (Applicant_Name, Experience_of_Applicant, Skill, Job_Application_ID)
        VALUES (NEW.Applicant_Name, NEW.Experience_of_Applicant, skill, NEW.Job_Application_ID);
    END WHILE;
END$$

DELIMITER ;
INSERT INTO job_applications (Applicant_Name, Experience_of_Applicant, Skills, Job_Application_ID)
VALUES ('John Doe', 5, 'Python,SQL,Power BI', 'J1001');

#OPEN POSITIONS TABLE SEGREGATION
SELECT * FROM open_positions;
CREATE TABLE IF NOT EXISTS open_positions_skills (
    Project_Name VARCHAR(20),
    Open_Positions VARCHAR(50),
    Skill VARCHAR(50),
    Job_Application_ID VARCHAR(20),
    PRIMARY KEY (Project_Name, Skill, Job_Application_ID),
    FOREIGN KEY (Job_Application_ID) REFERENCES open_positions(Job_Application_ID) ON DELETE CASCADE
);
#CREATING STORED PROCEEDURES
CREATE TABLE open_positions (
    Project_Name VARCHAR(100),
    Open_Positions VARCHAR(100),
    Key_Skills VARCHAR(255),
    Job_Application_ID VARCHAR(20) PRIMARY KEY
);
import pandas as pd
from sqlalchemy import create_engine

# Load updated Excel data
excel_path = r'C:\Users\USER\Desktop\project\SQL_Project_Sample_Data.xlsx'
excel_data = pd.read_excel(excel_path, sheet_name='Open_Positions')

# Connect to MySQL
engine = create_engine("mysql+pymysql://root:Golu%40123@localhost:3306/sql_project?charset=utf8mb4")

# Load existing data from MySQL
mysql_data = pd.read_sql('SELECT * FROM Open_Positions', con=engine)

# Find new entries
new_data = excel_data[~excel_data['Project_Name'].isin(mysql_data['Project_Name'])]

# Insert new data if available
if not new_data.empty:
    new_data.to_sql('open_positions', con=engine, if_exists='append', index=False)
    print("✅ New employee data inserted successfully into MySQL.")
else:
    print("🔍 No new employee data found.")
import pandas as pd
from sqlalchemy import create_engine

# Step 1: Connect to MySQL Database
engine = create_engine("mysql+pymysql://root:Golu%40123@localhost:3306/sql_project?charset=utf8mb4")
conn = engine.connect()
print("Connection successful")

# Step 2: Fetch Updated Data from MySQL
query = "SELECT * FROM Open_Positions"
updated_data = pd.read_sql(query, conn)

# Step 3: Sync Changes Back to Excel
excel_path = r'C:\Users\USER\Desktop\project\SQL_Project_Sample_Data.xlsx'

with pd.ExcelWriter(excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
    updated_data.to_excel(writer, sheet_name='Open_Positions', index=False)

# Step 4: Close the Database Connection
if 'conn' in locals():
    conn.close()
    print("Connection closed and data synced successfully to Excel.")
SET SQL_SAFE_UPDATES = 0;
CREATE TABLE IF NOT EXISTS job_applications_skills (
    Applicant_Name VARCHAR(50),
    Experience_of_Applicant int,
    Skill VARCHAR(50),
    Job_Application_ID VARCHAR(20),
    PRIMARY KEY (Applicant_Name, Skill, Job_Application_ID),
    FOREIGN KEY (Applicant_Name, Job_Application_ID) 
        REFERENCES job_applications(Applicant_Name, Job_Application_ID) 
        ON DELETE CASCADE
);
select * from job_applications;
select * from job_applications_skills;
DELIMITER $$

CREATE PROCEDURE InsertIntoJobApplicationsSkills()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_Applicant_Name VARCHAR(50);
    DECLARE v_Experience_of_Applicant INT;
    DECLARE v_Skills VARCHAR(150);
    DECLARE v_Job_Application_ID VARCHAR(20);
    DECLARE skill VARCHAR(50);
    DECLARE skill_cursor CURSOR FOR 
        SELECT Applicant_Name, Experience_of_Applicant, Skills, Job_Application_ID 
        FROM job_applications;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN skill_cursor;

    read_loop: LOOP
        FETCH skill_cursor INTO v_Applicant_Name, v_Experience_of_Applicant, v_Skills, v_Job_Application_ID;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Split skills and insert each one separately
        WHILE LENGTH(v_Skills) > 0 DO
            SET skill = SUBSTRING_INDEX(v_Skills, ',', 1);  -- Extract the first skill
            SET v_Skills = IF(LOCATE(',', v_Skills) > 0, SUBSTRING(v_Skills, LOCATE(',', v_Skills) + 1), '');  -- Remove the extracted skill

            -- Insert the record
            INSERT INTO job_applications_skills (Applicant_Name, Experience_of_Applicant, Skill, Job_Application_ID)
            VALUES (v_Applicant_Name, v_Experience_of_Applicant, skill, v_Job_Application_ID);
        END WHILE;
    END LOOP;

    CLOSE skill_cursor;
END$$

DELIMITER ;
CALL InsertIntoJobApplicationsSkills();
DELIMITER $$

CREATE TRIGGER trg_InsertJobApplicationsSkills
AFTER INSERT ON job_applications
FOR EACH ROW
BEGIN
    DECLARE skill VARCHAR(50);
    DECLARE remaining_skills VARCHAR(150);

    SET remaining_skills = NEW.Skills;  -- Get the skills from the inserted row

    -- Loop through each skill in the comma-separated list
    WHILE LENGTH(remaining_skills) > 0 DO
        SET skill = SUBSTRING_INDEX(remaining_skills, ',', 1);  -- Extract first skill
        SET remaining_skills = IF(LOCATE(',', remaining_skills) > 0, 
                                  SUBSTRING(remaining_skills, LOCATE(',', remaining_skills) + 1), 
                                  '');  -- Remove the extracted skill

        -- Insert each skill separately into job_applications_skills
        INSERT INTO job_applications_skills (Applicant_Name, Experience_of_Applicant, Skill, Job_Application_ID)
        VALUES (NEW.Applicant_Name, NEW.Experience_of_Applicant, skill, NEW.Job_Application_ID);
    END WHILE;
END$$

DELIMITER ;
INSERT INTO job_applications (Applicant_Name, Experience_of_Applicant, Skills, Job_Application_ID)
VALUES ('John Doe', 5, 'Python,SQL,Power BI', 'J1001');



CREATE TABLE IF NOT EXISTS open_positions_skills (
    Project_Name VARCHAR(20),
    Open_Positions VARCHAR(50),
    Skill VARCHAR(50),
    Job_Application_ID VARCHAR(20),
    PRIMARY KEY (Project_Name, Skill, Job_Application_ID),
    FOREIGN KEY (Job_Application_ID) REFERENCES open_positions(Job_Application_ID) ON DELETE CASCADE
);


DELIMITER $$


CREATE PROCEDURE InsertIntoOpenPositionsSkills()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_Project_Name VARCHAR(20);
    DECLARE v_Open_Positions VARCHAR(50);
    DECLARE v_Key_Skills VARCHAR(50);
    DECLARE v_Job_Application_ID VARCHAR(20);
    DECLARE skill VARCHAR(50);

    -- Cursor to loop through open_positions table
    DECLARE skill_cursor CURSOR FOR 
        SELECT Project_Name, Open_Positions, Key_Skills, Job_Application_ID 
        FROM open_positions;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN skill_cursor;

    read_loop: LOOP
        FETCH skill_cursor INTO v_Project_Name, v_Open_Positions, v_Key_Skills, v_Job_Application_ID;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Loop through each skill in the comma-separated list
        WHILE LENGTH(v_Key_Skills) > 0 DO
            SET skill = SUBSTRING_INDEX(v_Key_Skills, ',', 1);  -- Extract first skill
            SET v_Key_Skills = IF(LOCATE(',', v_Key_Skills) > 0, SUBSTRING(v_Key_Skills, LOCATE(',', v_Key_Skills) + 1), '');  -- Remove the extracted skill

            -- Insert each skill separately
            INSERT INTO open_positions_skills (Project_Name, Open_Positions, Skill, Job_Application_ID)
            VALUES (v_Project_Name, v_Open_Positions, skill, v_Job_Application_ID);
        END WHILE;
    END LOOP;

    CLOSE skill_cursor;
END$$

DELIMITER ;
#CALLING MANUALLY STORED PROCEDURES
CALL InsertIntoOpenPositionsSkills(); 
SELECT * FROM open_positions_skills;
#CREATING TRIGGER FOR AUTOMATION OF STORED PROCEDURES
DELIMITER $$

CREATE TRIGGER trg_InsertOpenPositionsSkills
AFTER INSERT ON open_positions
FOR EACH ROW
BEGIN
    DECLARE skill VARCHAR(50);
    DECLARE remaining_skills VARCHAR(50);

    SET remaining_skills = NEW.Key_Skills;  -- Get the skills from the inserted row

    -- Loop through each skill in the comma-separated list
    WHILE LENGTH(remaining_skills) > 0 DO
        SET skill = SUBSTRING_INDEX(remaining_skills, ',', 1);  -- Extract first skill
        SET remaining_skills = IF(LOCATE(',', remaining_skills) > 0, 
                                  SUBSTRING(remaining_skills, LOCATE(',', remaining_skills) + 1), 
                                  '');  -- Remove the extracted skill

        -- Insert each skill separately into open_positions_skills
        INSERT INTO open_positions_skills (Project_Name, Open_Positions, Skill, Job_Application_ID)
        VALUES (NEW.Project_Name, NEW.Open_Positions, skill, NEW.Job_Application_ID);
    END WHILE;
END$$

DELIMITER ;

#CHECKING IF IT IS INSERTED IN BOTH SQL AND EXCEL ,AFTER INSERTING THIS CODE ,RUN THE CODE IN PYTHON OF OPEN_POSITIONS
INSERT INTO open_positions (Project_Name, Open_Positions, Key_Skills, Job_Application_ID)
VALUES ('Project A', 'Data Scientist', 'Python,SQL,Machine Learning', 'D1001');



#NORMALIZATION
drop table office;


CREATE TABLE offices (
    Office_ID INT AUTO_INCREMENT PRIMARY KEY,
    Office_Location VARCHAR(50),
    We_Region VARCHAR(50)
);
CREATE TABLE employees (
    Employee_ID VARCHAR(25) PRIMARY KEY,
    Employee_Name VARCHAR(50),
    Salary BIGINT,
    Employee_Hire_Date DATE,
    Employee_Designation VARCHAR(50),
    Emp_Phone_No BIGINT,
    Office_ID INT,
    Employee_Gender VARCHAR(20),
    FOREIGN KEY (Office_ID) REFERENCES offices(Office_ID)
);
select * from employees;
drop table employees;
CREATE TABLE skills (
    Skill_ID INT AUTO_INCREMENT PRIMARY KEY,
    Skill_Name VARCHAR(50) UNIQUE
);
drop table skills;
CREATE TABLE clients (
    Client_ID VARCHAR(25) PRIMARY KEY,
    Client_Name VARCHAR(50)
);
CREATE TABLE projects (
    Project_ID VARCHAR(25) PRIMARY KEY,
    Project_Name VARCHAR(50),
    Client_ID VARCHAR(25),
    Project_Budget BIGINT,
    Project_Duration_in_Days BIGINT,
    FOREIGN KEY (Client_ID) REFERENCES clients(Client_ID)
);
CREATE TABLE employee_projects (
    Employee_ID VARCHAR(25),
    Project_ID VARCHAR(25),
    PRIMARY KEY (Employee_ID, Project_ID),
    FOREIGN KEY (Employee_ID) REFERENCES employees(Employee_ID),
    FOREIGN KEY (Project_ID) REFERENCES projects(Project_ID)
);
select * from employee_projects;
CREATE TABLE employee_skills (
    Employee_ID VARCHAR(25),
    Skill_ID INT,
    PRIMARY KEY (Employee_ID, Skill_ID),
    FOREIGN KEY (Employee_ID) REFERENCES employees(Employee_ID),
    FOREIGN KEY (Skill_ID) REFERENCES skills(Skill_ID)
);
#NOW WE CREATED STRUCTURE ,SO WE HAVE INSERT DATA FROM EXISTING TABLE
INSERT INTO offices (Office_Location, We_Region)
SELECT DISTINCT Office_Location, We_Region
FROM existing_employees;

select * from offices;
INSERT INTO employees (
    Employee_ID, Employee_Name, Salary, Employee_Hire_Date, 
    Employee_Designation, Emp_Phone_No, Office_ID, Employee_Gender
)
SELECT 
    e.Employee_ID, e.Employee_Name, e.Salary, e.Employee_Hire_Date, 
    e.Employee_Designation, e.Emp_Phone_No, o.Office_ID, e.Employee_Gender
FROM existing_employees e
JOIN offices o 
ON e.Office_Location = o.Office_Location AND e.We_Region = o.We_Region;

select * from employee_skills;
INSERT INTO employee_skills (Employee_ID, Skill_ID)
SELECT DISTINCT e.Employee_ID, s.Skill_ID
FROM existing_employees e
JOIN skills s ON FIND_IN_SET(s.Skill_Name, e.Employee_Skills) > 0;



INSERT INTO clients (Client_ID, Client_Name)
SELECT DISTINCT Client_ID, Client_Name
FROM existing_employees;
select * from clients;

INSERT INTO projects (Project_ID, Project_Name, Client_ID, Project_Budget, Project_Duration_in_Days)
SELECT DISTINCT Project_ID, Project_Name, Client_ID, Project_Budget, Project_Duration_in_Days
FROM existing_employees;
select * from projects;


INSERT INTO employee_projects (Employee_ID, Project_ID)
SELECT DISTINCT Employee_ID, Project_ID
FROM existing_employees;

select * from employee_projects;


SELECT distinct
	j.Applicant_Name,  
    j.Experience_of_Applicant, 
    -- j.Skill, 
    j.Job_Application_ID, 
    o.Project_Name, 
	o.Open_Positions
FROM job_applications_skills j  
JOIN open_positions_skills o  
ON j.Skill = o.Skill
AND j.Job_Application_ID=o.Job_Application_ID;


create table filter_candidate(
Applicant_Name varchar(50) primary key,
Experience_of_Applicant int,
Job_Application_ID varchar(20),
Project_Name varchar(30),
Open_Positions varchar(50));


insert into filter_candidate
select * from(SELECT distinct
	j.Applicant_Name,  
    j.Experience_of_Applicant, 
    -- j.Skill, 
    j.Job_Application_ID, 
    o.Project_Name, 
	o.Open_Positions
FROM job_applications_skills j  
JOIN open_positions_skills o  
ON j.Skill = o.Skill
AND j.Job_Application_ID=o.Job_Application_ID) k;

select * from filter_candidate;

DELIMITER //

CREATE PROCEDURE InsertIntoFilterCandidate()
BEGIN
    INSERT INTO filter_candidate (Applicant_Name, Experience_of_Applicant, Job_Application_ID, Project_Name, Open_Positions)
    SELECT DISTINCT
        j.Applicant_Name,  
        j.Experience_of_Applicant,  
        j.Job_Application_ID, 
        o.Project_Name, 
        o.Open_Positions
    FROM job_applications_skills j  
    JOIN open_positions_skills o  
    ON j.Skill = o.Skill
    AND j.Job_Application_ID = o.Job_Application_ID;
END //

DELIMITER ;

CALL InsertIntoFilterCandidate();

DELIMITER //

CREATE TRIGGER after_insert_job_applications_skills
AFTER INSERT ON job_applications_skills
FOR EACH ROW
BEGIN
    CALL InsertIntoFilterCandidate();
END //

CREATE TRIGGER after_insert_open_positions_skills
AFTER INSERT ON open_positions_skills
FOR EACH ROW
BEGIN
    CALL InsertIntoFilterCandidate();
END //

DELIMITER ;

CREATE TABLE IF NOT EXISTS interview_process (
    Applicant_Name VARCHAR(50),
    Job_Application_ID VARCHAR(20),
    Round INT,
    Status ENUM('Yes', 'No'),
    PRIMARY KEY (Applicant_Name, Job_Application_ID, Round)
);

CREATE TABLE IF NOT EXISTS final_candidate (
    Applicant_Name VARCHAR(50),
    Job_Application_ID VARCHAR(20),
    Status ENUM('Selected'),
    PRIMARY KEY (Applicant_Name, Job_Application_ID)
);

DELIMITER //
drop PROCEDURE SmartUpdateInterviewProcessStatus;
CREATE PROCEDURE SmartUpdateInterviewProcessStatus(
    IN p_Applicant_Name VARCHAR(50),
    IN p_Job_Application_ID VARCHAR(20),
    IN p_Round INT,
    IN p_Status ENUM('Yes', 'No')
)
BEGIN
    DECLARE round1_status ENUM('Yes', 'No');
    DECLARE round2_status ENUM('Yes', 'No');

    -- Insert or update status for the round
    INSERT INTO interview_process(Applicant_Name, Job_Application_ID, Round, Status)
    VALUES (p_Applicant_Name, p_Job_Application_ID, p_Round, p_Status)
    ON DUPLICATE KEY UPDATE Status = p_Status;

    -- Check if candidate qualifies for final_candidate
    IF p_Round = 3 THEN
        -- Get previous round statuses
        SELECT Status INTO round1_status
        FROM interview_process
        WHERE Applicant_Name = p_Applicant_Name AND Job_Application_ID = p_Job_Application_ID AND Round = 1;

        SELECT Status INTO round2_status
        FROM interview_process
        WHERE Applicant_Name = p_Applicant_Name AND Job_Application_ID = p_Job_Application_ID AND Round = 2;

        -- Insert into final_candidate if all rounds passed
        IF round1_status = 'Yes' AND round2_status = 'Yes' AND p_Status = 'Yes' THEN
            INSERT INTO final_candidate(Applicant_Name, Job_Application_ID, Status)
            VALUES (p_Applicant_Name, p_Job_Application_ID, 'Selected')
            ON DUPLICATE KEY UPDATE Status = 'Selected';
        END IF;
    END IF;
END //

DELIMITER ;


-- Checking
select * from filter_candidate;
-- Round 1 passed
CALL SmartUpdateInterviewProcessStatus('Isla Espinoza', 'J1002', 1, 'Yes');

-- Round 2 passed
CALL SmartUpdateInterviewProcessStatus('Isla Espinoza', 'J1002', 2, 'Yes');

-- Round 3 passed → gets added to final_candidate
CALL SmartUpdateInterviewProcessStatus('Isla Espinoza', 'J1002', 3, 'Yes');

-- See interview round statuses
SELECT * FROM interview_process;

-- See final selection result
SELECT * FROM final_candidate;

CREATE TABLE IF NOT EXISTS final_candidate (
    Applicant_Name VARCHAR(50),
    Job_Application_ID VARCHAR(20),
    Status ENUM('Selected'),
    PRIMARY KEY (Applicant_Name, Job_Application_ID)
);



DELIMITER //

CREATE PROCEDURE SmartUpdateInterviewProcessStatus(
    IN p_Applicant_Name VARCHAR(50),
    IN p_Job_Application_ID VARCHAR(20),
    IN p_Round INT,
    IN p_Status VARCHAR(3)  -- Changed ENUM to VARCHAR
)
BEGIN
    DECLARE round1_status VARCHAR(3);
    DECLARE round2_status VARCHAR(3);

    -- Insert or update status for the round
    INSERT INTO interview_process(Applicant_Name, Job_Application_ID, Round, Status)
    VALUES (p_Applicant_Name, p_Job_Application_ID, p_Round, p_Status)
    ON DUPLICATE KEY UPDATE Status = p_Status;

    -- Check if candidate qualifies for final_candidate
    IF p_Round = 3 THEN
        SELECT Status INTO round1_status
        FROM interview_process
        WHERE Applicant_Name = p_Applicant_Name AND Job_Application_ID = p_Job_Application_ID AND Round = 1;

        SELECT Status INTO round2_status
        FROM interview_process
        WHERE Applicant_Name = p_Applicant_Name AND Job_Application_ID = p_Job_Application_ID AND Round = 2;

        IF round1_status = 'Yes' AND round2_status = 'Yes' AND p_Status = 'Yes' THEN
            INSERT INTO final_candidate(Applicant_Name, Job_Application_ID, Status)
            VALUES (p_Applicant_Name, p_Job_Application_ID, 'Selected')
            ON DUPLICATE KEY UPDATE Status = 'Selected';
        END IF;
    END IF;
END //

DELIMITER ;


use sql_project;

CREATE TABLE IF NOT EXISTS project_budget_tracker (
    project_id VARCHAR(25) PRIMARY KEY,
    remaining_budget BIGINT
);


INSERT INTO project_budget_tracker (project_id, remaining_budget)
SELECT 
    p.Project_ID,
    (p.Project_Budget - IFNULL(SUM(e.Salary), 0)) AS remaining_budget
FROM projects p
LEFT JOIN employee_projects ep ON p.Project_ID = ep.Project_ID
LEFT JOIN employees e ON ep.Employee_ID = e.Employee_ID
GROUP BY p.Project_ID
ON DUPLICATE KEY UPDATE 
    remaining_budget = VALUES(remaining_budget);

#INSERT INTO project_budget_tracker (project_id, remaining_budget)
#SELECT 
#    p.Project_ID,
 #FROM projects p
#LEFT JOIN employee_projects ep ON p.Project_ID = ep.Project_ID
#LEFT JOIN employees e ON ep.Employee_ID = e.Employee_ID
#GROUP BY p.Project_ID;



CREATE TABLE IF NOT EXISTS new_employee_salary (
    employee_id VARCHAR(20) PRIMARY KEY,
    employee_name VARCHAR(50),
    project_id VARCHAR(25),
    project_name VARCHAR(50),
    salary BIGINT,
    remaining_budget BIGINT
);


DELIMITER //

CREATE PROCEDURE insert_new_employee_salary_sequential(
    IN p_job_app_id VARCHAR(20), 
    IN p_applicant_name VARCHAR(50)
)
BEGIN
    DECLARE v_project_id VARCHAR(25);
    DECLARE v_project_name VARCHAR(50);
    DECLARE v_remaining_budget BIGINT;
    DECLARE v_salary BIGINT;
    DECLARE v_final_budget BIGINT;

    -- 1. Get project with highest remaining budget
    SELECT 
        pbt.project_id,
        p.Project_Name,
        pbt.remaining_budget
    INTO 
        v_project_id,
        v_project_name,
        v_remaining_budget
    FROM project_budget_tracker pbt
    JOIN projects p ON pbt.project_id = p.Project_ID
    ORDER BY pbt.remaining_budget DESC
    LIMIT 1;

    -- 2. Calculate salary = 30% of remaining budget
    SET v_salary = ROUND(v_remaining_budget * 0.3);

    -- 3. Update remaining budget
    SET v_final_budget = v_remaining_budget - v_salary;

    -- 4. Insert into new_employee_salary
    INSERT INTO new_employee_salary(employee_id, employee_name, project_id, project_name, salary, remaining_budget)
    VALUES (p_job_app_id, p_applicant_name, v_project_id, v_project_name, v_salary, v_final_budget);

    -- 5. Update budget tracker
    UPDATE project_budget_tracker
    SET remaining_budget = v_final_budget
    WHERE project_id = v_project_id;
END //

DELIMITER ;


DELIMITER //

CREATE TRIGGER trg_after_selected_candidate_insert
AFTER INSERT ON final_candidate
FOR EACH ROW
BEGIN
    IF NEW.Status = 'Selected' THEN
        CALL insert_new_employee_salary_sequential(NEW.Job_Application_ID, NEW.Applicant_Name);
    END IF;
END //

DELIMITER ;



DELIMITER //

CREATE TRIGGER trg_after_selected_candidate_update
AFTER UPDATE ON final_candidate
FOR EACH ROW
BEGIN
    -- If changed to "Selected", insert
    IF NEW.Status = 'Selected' AND OLD.Status != 'Selected' THEN
        CALL insert_new_employee_salary_sequential(NEW.Job_Application_ID, NEW.Applicant_Name);
    END IF;

    -- If changed from "Selected" to something else, delete
    IF NEW.Status != 'Selected' AND OLD.Status = 'Selected' THEN
        DELETE FROM new_employee_salary
        WHERE employee_id = OLD.Job_Application_ID;
    END IF;
END //

DELIMITER ;


select * from new_employee_salary;
select * from project_budget_tracker;
select * from existing_employees;


-- Insert sample interview rounds for a candidate
CALL SmartUpdateInterviewProcessStatus('Emma Watson', 'J1009', 1, 'Yes');
CALL SmartUpdateInterviewProcessStatus('Emma Watson', 'J1009', 2, 'Yes');
CALL SmartUpdateInterviewProcessStatus('Emma Watson', 'J1009', 3, 'Yes');

-- Final candidate list
SELECT * FROM final_candidate;

-- Salary assignment
SELECT * FROM new_employee_salary;

-- Updated budget status
SELECT * FROM project_budget_tracker;


CREATE OR REPLACE VIEW final_hiring_report AS
SELECT 
    f.Applicant_Name,
    f.Job_Application_ID,
    n.project_name,
    n.salary,
    n.remaining_budget,
    i1.Status AS Round1,
    i2.Status AS Round2,
    i3.Status AS Round3
FROM final_candidate f
JOIN new_employee_salary n ON f.Job_Application_ID = n.employee_id
LEFT JOIN interview_process i1 ON f.Applicant_Name = i1.Applicant_Name AND i1.Round = 1
LEFT JOIN interview_process i2 ON f.Applicant_Name = i2.Applicant_Name AND i2.Round = 2
LEFT JOIN interview_process i3 ON f.Applicant_Name = i3.Applicant_Name AND i3.Round = 3;
