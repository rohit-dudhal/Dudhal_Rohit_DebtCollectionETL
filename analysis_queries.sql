-- What is the average loan amount for borrowers who are more than 5 days past due?
SELECT AVG(loan_amount) AS average_loan_amount 
FROM predixion.borrower 
WHERE days_left_to_pay_current_emi > 5;

-- Who are the top 10 borrowers with the highest outstanding balance?
select name, loan_amount-(emi*total_emi_paid_count) as outstanding_balance 
from predixion.borrower 
order by 2 desc limit 10;

-- List of all borrowers with good repayment history
select name 
from predixion.borrower 
where delayed_payment='No';

-- Count of Borrowers for Each Loan Type
SELECT loan_type, COUNT(*) AS borrower_count 
FROM predixion.borrower 
GROUP BY loan_type;

-- Average Loan Amount for Each Loan Type
SELECT loan_type, AVG(loan_amount) AS average_loan_amount 
FROM predixion.borrower 
GROUP BY loan_type;

-- Detailed Statistics for Each Loan Type
SELECT loan_type, COUNT(*) AS borrower_count,AVG(loan_amount) AS average_loan_amount,
round(SUM(loan_amount-(emi*total_emi_paid_count)),2) AS total_outstanding_balance 
FROM predixion.borrower GROUP BY loan_type;

-- Top Borrowers by Loan Type	
SELECT loan_type, name, outstanding_amount 
FROM ( 
    SELECT loan_type, name, loan_amount-(emi*total_emi_paid_count) as outstanding_amount,
    ROW_NUMBER() OVER (PARTITION BY loan_type ORDER BY loan_amount-(emi*total_emi_paid_count) DESC) AS rn 
    FROM predixion.borrower) 
AS ranked WHERE rn = 1;