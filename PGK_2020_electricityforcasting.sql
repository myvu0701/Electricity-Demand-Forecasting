--Package name:         PKG_2020MAIN
--Author:               Thi Thao My VU
--Student ID:           12862177
--Date:                 05-June-2020

-- The following package PKG_2020MAIN include, package Spec and packeage body.

--Package spec contain: 
--Function is_holiday: consider if the given day is holiday
--Function has_holiday_data: consider if there is volume consumption on holiday in v_nem_rm16 table
--PROCEDURE RM16_forecast: procedure that loop from day 1 to day 14, to forecast the average consumption volume for 14 days into the future. 

--Package body contain:
--Function is_holiday: consider if the given day is holiday.
--Function has_holiday_data: consider if there is volume consumption on holiday in v_nem_rm16 table.
--Procedure change_type_and_date: This procedure change the statement type and input change date.
--Procedure forecast_normal_day:  forecast average volume for normal day, and insert it into local_rm16.
--Procedure forecast_holiday_yesdata:  forecast average volume for holidays when there is past holiday data, and insert it into local_rm16. 
--Procedure forecast_holiday_nodata:  forecast average volume for holidays when there no past holiday data, so use data on SUNDAY and insert it into local_rm16
--PROCEDURE RM16_forecast_run: This procedure performs the forecasting,  loop from day 1 to day 14, to forecast the average consumption volume for 14 days into the future.
--PROCEDURE RM16_forecast: procedure that using all functions,  procedures above. Also, make sure thr program run once per day.


--This is package SPEC - public
Create OR REPLACE PACKAGE PKG_2020MAIN AS
        FUNCTION is_holiday(forecast_date IN DATE) RETURN BOOLEAN;
        FUNCTION has_holiday_data RETURN BOOLEAN;
        PROCEDURE RM16_forecast_run;
        PROCEDURE RM16_forecast;
         v_package_name VARCHAR2(35) := 'PKG_2020MAIN';
         
       END PKG_2020MAIN;
/
 --This in package body - private
CREATE OR REPLACE PACKAGE BODY PKG_2020MAIN AS

    --This function checks if the date is holiday
    FUNCTION is_holiday(forecast_date IN DATE) RETURN BOOLEAN IS
        is_holiday boolean := false;
        BEGIN
            for c1 in ( select * from DBP_HOLIDAY where TRUNC(HOLIDAY_DATE) = TRUNC(forecast_date) ) loop
            is_holiday := true;
            exit;
            end loop;
            return( is_holiday );
        end ;
    
    --This function checks if there is holiday data
    FUNCTION has_holiday_data RETURN BOOLEAN IS
        has_holiday_data boolean := false;
        BEGIN
            for c2 in (SELECT * FROM v_nem_rm16 a INNER JOIN DBP_holiday b ON a.day= b.holiday_date) loop
            has_holiday_data := true;
            exit;
            end loop;
            return(has_holiday_data);
        end ;
    
    --This procedure change the statement type and input change date
    PROCEDURE change_type_and_date IS
    BEGIN
            UPDATE LOCAL_RM16
            SET STATEMENT_TYPE = 'FORECAST', 
            change_date = SYSDATE
            WHERE STATEMENT_TYPE IS NULL AND CHANGE_DATE IS NULL;
        END;
        
    --This procedure forecast for normal day
    PROCEDURE forecast_normal_day (forecast_date IN DATE) IS
     BEGIN
            INSERT INTO LOCAL_RM16(TNI, FRMP, LR, HH, DAY,  VOLUME)
            SELECT  TNI, FRMP, LR, HH,forecast_date,
            AVG(VOLUME) 
            FROM v_nem_rm16
            WHERE TO_CHAR(TRUNC(DAY),'DAY') = TO_CHAR(TRUNC(forecast_date),'DAY') 
            AND TRUNC(v_nem_rm16.DAY) NOT IN (SELECT TRUNC(HOLIDAY_DATE) FROM DBP_HOLIDAY)
            AND statement_type != 'FORECAST'
            GROUP BY TNI, FRMP, LR, HH;
            change_type_and_date;
            
            COMMIT;
                Common.log('Forecasting ' || forecast_date|| '- Normal Day: DONE ');
                EXCEPTION WHEN OTHERS THEN
                Common.log('Forecasting '|| forecast_date || '- Normal Day: ERROR with ' ||SQLERRM);
                ROLLBACK;
        end ;
        
    --This procedure forecasts for holidays and there is past data
    PROCEDURE forecast_holiday_yesdata (forecast_date IN DATE) IS
        BEGIN
            INSERT INTO LOCAL_RM16(TNI, FRMP, LR, HH, DAY,  VOLUME)
            SELECT  TNI, FRMP, LR, HH,forecast_date,
            AVG(VOLUME) 
            FROM v_nem_rm16
            WHERE  TRUNC(v_nem_rm16.DAY) IN (SELECT TRUNC(HOLIDAY_DATE) FROM DBP_HOLIDAY)
            AND statement_type != 'FORECAST'
            GROUP BY TNI, FRMP, LR, HH;
            change_type_and_date;
            
            COMMIT;
                Common.log('Forecasting ' || forecast_date || '- Holiday using Past Data: DONE ');
                EXCEPTION WHEN OTHERS THEN
                Common.log('Forecasting '|| forecast_date || '- Holiday using Past Data: ERROR with '||SQLERRM);
                ROLLBACK;
        end ;
    
    --This procedure forecasts for holidays and there is NO past data
    PROCEDURE forecast_holiday_nodata (forecast_date IN DATE) IS 
        BEGIN
            INSERT INTO LOCAL_RM16(TNI, FRMP, LR, HH, DAY,  VOLUME)
            
            SELECT  TNI, FRMP, LR, HH,forecast_date,
            AVG(VOLUME) 
            FROM v_nem_rm16
            WHERE  TO_CHAR(TRUNC(DAY),'DAY') like '%SUNDAY%'
            AND statement_type != 'FORECAST'
            GROUP BY TNI, FRMP, LR, HH ;
            change_type_and_date;
            
            COMMIT;
                Common.log('Forecasting ' || forecast_date || '- Holiday using Sundays: DONE ');
                EXCEPTION WHEN OTHERS THEN
                Common.log('Forecasting ' || forecast_date || '- Holiday using Sundays: ERROR with '||SQLERRM);
                ROLLBACK;
        end ;
        
    --This procedure performs the forecasting
     PROCEDURE RM16_forecast_run IS
        BEGIN
            FOR i IN 1..14 LOOP
                IF NOT is_holiday(SYSDATE + i) THEN forecast_normal_day(SYSDATE + i);
                    ELSIF has_holiday_data THEN forecast_holiday_yesdata(SYSDATE + i); 
                    ELSE  forecast_holiday_nodata(SYSDATE + i);
                END IF;
            END LOOP;
        END;
        
    -- This procedure create run table.
        procedure RM16_forecast IS
            v_runlogrecord run_table%ROWTYPE;
            v_runlogID NUMBER;
            --    v_unique VARCHAR(20) := to_char(sysdate, 'DDMMYYYY') || '000';
            moduleRan EXCEPTION;
            c_buffer CONSTANT NUMBER :=1;
            --    c_moduleName VARCHAR(25) :='RUN_TABLE';
    
            Begin    
                Begin
                    select * INTO v_runlogrecord
                    from run_table
                    where outcome = 'SUCCESS'
                    and Run_End > (sysdate - c_buffer);
                
                    RAISE moduleRan;
                    
                    Exception
                        
                            WHEN NO_DATA_FOUND then
                            SELECT seq_runID.NEXTVAL INTO v_runLogID from dual;
                            --            v_runlogID := v_unique + v_runlogID;
                            INSERT INTO run_table(runID, Run_Start, Run_End, Outcome, Remarks)
                            VALUES(v_runlogID, sysdate, NULL, NULL, 'Start Program');
                    END;
               
                    rm16_forecast_run;
              
                 UPDATE run_table 
                                set run_end = sysdate,
                                outcome = 'SUCCESS',
                                remarks = 'Run Completed'
                                where runid = v_runlogID;
            
                EXCEPTION 
                    WHEN moduleRan THEN
                    DBMS_OUTPUT.PUT_LINE('Forecast has been done');
                    DBMS_OUTPUT.PUT_LINE('Please check your run_log table and run tomorrow');
                
            END;

END PKG_2020MAIN;


-- In order to run the PKG_2020MAIN, use the command below:
SET SERVEROUTPUT ON
BEGIN
    PKG_2020MAIN.RM16_forecast;
END;

--To check forecasted 14 days into the future data that stored local_rm16:
select * from local_rm16;
-- To check the run_table:
select * from run_table;

