/*********************************************************************
    Author  :   TothF  
    Remark  :   Time Machine
    Date    :   2024.01.12
    
    Restrictions:
     there is no table drop, only if there is no data in it
     no column drop, only if there is no data in it
     no column type changes
     there is no PK or FK or other check constraints
     there are no extra indexes    
     
*********************************************************************/


Prompt ****************************************************************
Prompt **       I N S T A L L I N G    T I M E   M A C H I N E       **
Prompt ****************************************************************
/*
drop sequence TIMEMACHINE_SEQUENCE;
drop table TIMEMACHINE_NUMBERS;
drop table TIMEMACHINE_STRINGS;
drop table TIMEMACHINE_DATES;
drop table TIMEMACHINE_TIMESTAMPS;
*/

Prompt ***************************************************************
Prompt **                      S E Q U E N C E                      **
Prompt ***************************************************************
/*   To trigger numbering  */
create sequence TIMEMACHINE_SEQUENCE
    Increment by           1
    Minvalue         1000000
    Maxvalue 999999999999999
    Start With       1000000
    NoCycle
    NoCache;


Prompt ***************************************************************
Prompt **        S E S S I O N    V A R I A B L E S                 **
Prompt ***************************************************************

create or replace package PKG_TM as

    G_NOW           timestamp;         /* null = current = sys_extract_utc( systimestamp ) */
    G_FOREVER       timestamp := to_timestamp('9999.01.01', 'yyyy.mm.dd');
    G_TRANSACTION   varchar2(300);     /* optionaly set this before DML command */
    G_CRLF          varchar(2)  := chr(13)||chr(10);  -- $0D0A  CrLf

end;
/

-----------------------------------------------------------------------------------------------
 
create or replace procedure TM_SET_NOW ( I_NOW in timestamp default null ) as
begin
    PKG_TM.G_NOW := I_NOW;
end;
/

-----------------------------------------------------------------------------------------------

create or replace function TM_NOW return timestamp as
    pragma UDF;
begin
    return nvl( PKG_TM.G_NOW, sys_extract_utc( systimestamp ) );
end;
/

-----------------------------------------------------------------------------------------------

create or replace procedure TM_SET_TRANSACTION ( I_TRANSACTION in varchar2 ) as
begin
    PKG_TM.G_TRANSACTION := I_TRANSACTION;
end;
/    

-----------------------------------------------------------------------------------------------

create or replace function TM_TRANSACTION return timestamp as
begin
    return PKG_TM.G_TRANSACTION;
end;
/    


Prompt *****************************************************************
Prompt **                   D A T A    T A B L E S                    **
Prompt *****************************************************************

CREATE TABLE TIMEMACHINE_NUMBERS (
  TABLE_ID                  NUMBER          CONSTRAINT TIMEMACHINE_NUMBERS_NN01 NOT NULL,
  COLUMN_ID                 NUMBER          CONSTRAINT TIMEMACHINE_NUMBERS_NN02 NOT NULL,
  ROW_ID                    NUMBER          CONSTRAINT TIMEMACHINE_NUMBERS_NN03 NOT NULL, 
  CREATED_AT                TIMESTAMP       CONSTRAINT TIMEMACHINE_NUMBERS_NN04 NOT NULL,
  CLOSED_AT                 TIMESTAMP       CONSTRAINT TIMEMACHINE_NUMBERS_NN05 NOT NULL,
  VALUE                     NUMBER,
  CREATED_BY                VARCHAR2(300),
  CLOSED_BY                 VARCHAR2(300),
  CONSTRAINT TIMEMACHINE_NUMBERS_PK         PRIMARY KEY ( TABLE_ID, ROW_ID, COLUMN_ID, CREATED_AT, CLOSED_AT ) 
  )
ORGANIZATION INDEX;


-----------------------------------------------------------------------------------------------

CREATE TABLE TIMEMACHINE_STRINGS (
  TABLE_ID                  NUMBER          CONSTRAINT TIMEMACHINE_STRINGS_NN01 NOT NULL,
  COLUMN_ID                 NUMBER          CONSTRAINT TIMEMACHINE_STRINGS_NN02 NOT NULL,
  ROW_ID                    NUMBER          CONSTRAINT TIMEMACHINE_STRINGS_NN03 NOT NULL, 
  CREATED_AT                TIMESTAMP       CONSTRAINT TIMEMACHINE_STRINGS_NN04 NOT NULL,
  CLOSED_AT                 TIMESTAMP       CONSTRAINT TIMEMACHINE_STRINGS_NN05 NOT NULL,
  VALUE                     VARCHAR2(4000),
  CREATED_BY                VARCHAR2(300),
  CLOSED_BY                 VARCHAR2(300),
  CONSTRAINT TIMEMACHINE_STRINGS_PK         PRIMARY KEY ( TABLE_ID, ROW_ID, COLUMN_ID, CREATED_AT, CLOSED_AT ) 
  )
ORGANIZATION INDEX
OVERFLOW TABLESPACE USERS;

-----------------------------------------------------------------------------------------------

CREATE TABLE TIMEMACHINE_DATES (
  TABLE_ID                  NUMBER          CONSTRAINT TIMEMACHINE_DATES_NN01 NOT NULL,
  COLUMN_ID                 NUMBER          CONSTRAINT TIMEMACHINE_DATES_NN02 NOT NULL,
  ROW_ID                    NUMBER          CONSTRAINT TIMEMACHINE_DATES_NN03 NOT NULL, 
  CREATED_AT                TIMESTAMP       CONSTRAINT TIMEMACHINE_DATES_NN04 NOT NULL,
  CLOSED_AT                 TIMESTAMP       CONSTRAINT TIMEMACHINE_DATES_NN05 NOT NULL,
  VALUE                     DATE,
  CREATED_BY                VARCHAR2(300),
  CLOSED_BY                 VARCHAR2(300),
  CONSTRAINT TIMEMACHINE_DATES_PK           PRIMARY KEY ( TABLE_ID, ROW_ID, COLUMN_ID, CREATED_AT, CLOSED_AT ) 
  )
ORGANIZATION INDEX;

-----------------------------------------------------------------------------------------------

CREATE TABLE TIMEMACHINE_TIMESTAMPS (
  TABLE_ID                  NUMBER          CONSTRAINT TIMEMACHINE_TIMESTAMPS_NN01 NOT NULL,
  COLUMN_ID                 NUMBER          CONSTRAINT TIMEMACHINE_TIMESTAMPS_NN02 NOT NULL,
  ROW_ID                    NUMBER          CONSTRAINT TIMEMACHINE_TIMESTAMPS_NN03 NOT NULL, 
  CREATED_AT                TIMESTAMP       CONSTRAINT TIMEMACHINE_TIMESTAMPS_NN04 NOT NULL,
  CLOSED_AT                 TIMESTAMP       CONSTRAINT TIMEMACHINE_TIMESTAMPS_NN05 NOT NULL,
  VALUE                     TIMESTAMP,
  CREATED_BY                VARCHAR2(300),
  CLOSED_BY                 VARCHAR2(300),
CONSTRAINT TIMEMACHINE_TIMESTAMPS_PK        PRIMARY KEY ( TABLE_ID, ROW_ID, COLUMN_ID, CREATED_AT, CLOSED_AT ) 
  )
ORGANIZATION INDEX;


Prompt *****************************************************************
Prompt **                      TM_DATA_TYPES                          **
Prompt *****************************************************************

CREATE OR REPLACE VIEW TM_DATA_TYPES AS
SELECT DATA_TYPE_NAME.ROW_ID         AS ROW_ID
     , DATA_TYPE_NAME.VALUE          AS DATA_TYPE_NAME
     , DATA_TABLE_NAME.VALUE         AS DATA_TABLE_NAME  
     , DESCRIPTION.VALUE             AS DESCRIPTION
  FROM      TIMEMACHINE_STRINGS DATA_TYPE_NAME
  LEFT JOIN TIMEMACHINE_STRINGS DATA_TABLE_NAME ON ( DATA_TYPE_NAME.TABLE_ID = DATA_TABLE_NAME.TABLE_ID AND DATA_TYPE_NAME.ROW_ID = DATA_TABLE_NAME.ROW_ID AND DATA_TABLE_NAME.COLUMN_ID = 2 AND TM_NOW BETWEEN DATA_TABLE_NAME.CREATED_AT AND DATA_TABLE_NAME.CLOSED_AT )
  LEFT JOIN TIMEMACHINE_STRINGS DESCRIPTION     ON ( DATA_TYPE_NAME.TABLE_ID = DESCRIPTION.TABLE_ID     AND DATA_TYPE_NAME.ROW_ID = DESCRIPTION.ROW_ID     AND DESCRIPTION.COLUMN_ID     = 3 AND TM_NOW BETWEEN DESCRIPTION.CREATED_AT     AND DESCRIPTION.CLOSED_AT     )
 WHERE DATA_TYPE_NAME.TABLE_ID  = 1 
   AND DATA_TYPE_NAME.COLUMN_ID = 1 
   AND TM_NOW BETWEEN DATA_TYPE_NAME.CREATED_AT AND DATA_TYPE_NAME.CLOSED_AT 
;

-----------------------------------------------------------------------------------------------

CREATE OR REPLACE TRIGGER TRG_TM_DATA_TYPES_IIR 
   INSTEAD OF INSERT ON TM_DATA_TYPES FOR EACH ROW
DECLARE
    V_ROW_ID    NUMBER;
    V_NOW       TIMESTAMP := SYS_EXTRACT_UTC( SYSTIMESTAMP );
BEGIN
    IF PKG_TM_NOW.G_NOW IS NULL THEN
        V_ROW_ID := NVL( :NEW.ROW_ID, TIMEMACHINE_SEQUENCE.NEXTVAL );
        IF :NEW.DATA_TYPE_NAME  IS NOT NULL THEN INSERT INTO TIMEMACHINE_STRINGS ( TABLE_ID, COLUMN_ID, ROW_ID, CREATED_AT, CLOSED_AT, VALUE, CREATED_BY ) VALUES ( 1, 1, V_ROW_ID, V_NOW, PKG_TM_NOW.G_FOREVER, :NEW.DATA_TYPE_NAME  , TM_TRANSACTION ); END IF;
        IF :NEW.DATA_TABLE_NAME IS NOT NULL THEN INSERT INTO TIMEMACHINE_STRINGS ( TABLE_ID, COLUMN_ID, ROW_ID, CREATED_AT, CLOSED_AT, VALUE, CREATED_BY ) VALUES ( 1, 2, V_ROW_ID, V_NOW, PKG_TM_NOW.G_FOREVER, :NEW.DATA_TABLE_NAME , TM_TRANSACTION ); END IF;
        IF :NEW.DESCRIPTION     IS NOT NULL THEN INSERT INTO TIMEMACHINE_STRINGS ( TABLE_ID, COLUMN_ID, ROW_ID, CREATED_AT, CLOSED_AT, VALUE, CREATED_BY ) VALUES ( 1, 3, V_ROW_ID, V_NOW, PKG_TM_NOW.G_FOREVER, :NEW.DESCRIPTION     , TM_TRANSACTION ); END IF;
    END IF;    
END;
/   

-----------------------------------------------------------------------------------------------

INSERT INTO TM_DATA_TYPES ( ROW_ID, DATA_TYPE_NAME, DATA_TABLE_NAME, DESCRIPTION ) VALUES ( 1, 'NUMBER'   ,'TIMEMACHINE_NUMBERS'   , 'Every NUMBER types'                    );
INSERT INTO TM_DATA_TYPES ( ROW_ID, DATA_TYPE_NAME, DATA_TABLE_NAME, DESCRIPTION ) VALUES ( 2, 'STRING'   ,'TIMEMACHINE_STRINGS'   , 'Every CHAR, VARCHAR, so string types'  );
INSERT INTO TM_DATA_TYPES ( ROW_ID, DATA_TYPE_NAME, DATA_TABLE_NAME, DESCRIPTION ) VALUES ( 3, 'DATE'     ,'TIMEMACHINE_DATES'     , 'DATE data type'                        );
INSERT INTO TM_DATA_TYPES ( ROW_ID, DATA_TYPE_NAME, DATA_TABLE_NAME, DESCRIPTION ) VALUES ( 4, 'TIMESTAMP','TIMEMACHINE_TIMESTAMPS', 'TIMESTAMP data type'                   );
COMMIT;



Prompt *****************************************************************
Prompt **                      TM_TABLES                              **
Prompt *****************************************************************

CREATE OR REPLACE VIEW TM_TABLES     AS
SELECT TABLE_NAME.ROW_ID             AS ROW_ID
     , TABLE_NAME.VALUE              AS TABLE_NAME  
     , DESCRIPTION.VALUE             AS DESCRIPTION
  FROM      TIMEMACHINE_STRINGS TABLE_NAME
  LEFT JOIN TIMEMACHINE_STRINGS DESCRIPTION ON ( TABLE_NAME.TABLE_ID = DESCRIPTION.TABLE_ID AND TABLE_NAME.ROW_ID = DESCRIPTION.ROW_ID AND DESCRIPTION.COLUMN_ID = 5 AND TM_NOW BETWEEN DESCRIPTION.CREATED_AT AND DESCRIPTION.CLOSED_AT )
 WHERE TABLE_NAME.TABLE_ID  = 2 
   AND TABLE_NAME.COLUMN_ID = 4 
   AND TM_NOW BETWEEN TABLE_NAME.CREATED_AT AND TABLE_NAME.CLOSED_AT 
;

-----------------------------------------------------------------------------------------------

CREATE OR REPLACE TRIGGER TRG_TM_TABLES_IIR 
   INSTEAD OF INSERT ON TM_TABLES FOR EACH ROW
DECLARE
    V_ROW_ID    NUMBER;
    V_NOW       TIMESTAMP := SYS_EXTRACT_UTC( SYSTIMESTAMP );
BEGIN
    IF PKG_TM_NOW.G_NOW IS NULL THEN
        V_ROW_ID := NVL( :NEW.ROW_ID, TIMEMACHINE_SEQUENCE.NEXTVAL );
        IF :NEW.TABLE_NAME  IS NOT NULL THEN INSERT INTO TIMEMACHINE_STRINGS ( TABLE_ID, COLUMN_ID, ROW_ID, CREATED_AT, CLOSED_AT, VALUE, CREATED_BY ) VALUES ( 2, 4, V_ROW_ID, V_NOW, PKG_TM_NOW.G_FOREVER, :NEW.TABLE_NAME  , TM_TRANSACTION ); END IF;
        IF :NEW.DESCRIPTION IS NOT NULL THEN INSERT INTO TIMEMACHINE_STRINGS ( TABLE_ID, COLUMN_ID, ROW_ID, CREATED_AT, CLOSED_AT, VALUE, CREATED_BY ) VALUES ( 2, 5, V_ROW_ID, V_NOW, PKG_TM_NOW.G_FOREVER, :NEW.DESCRIPTION , TM_TRANSACTION ); END IF;
    END IF;    
END;
/   

-----------------------------------------------------------------------------------------------

INSERT INTO TM_TABLES ( ROW_ID, TABLE_NAME, DESCRIPTION ) VALUES ( 1, 'TM_DATA_TYPES'   , 'Data types and its physical table names' );
INSERT INTO TM_TABLES ( ROW_ID, TABLE_NAME, DESCRIPTION ) VALUES ( 2, 'TM_TABLES'       , 'The name of TM managed tables' );
INSERT INTO TM_TABLES ( ROW_ID, TABLE_NAME, DESCRIPTION ) VALUES ( 3, 'TM_TABLE_COLUMNS', 'The table columns of TM managed tables' );
COMMIT;



Prompt *****************************************************************
Prompt **                    TM_TABLE_COLUMNS                         **
Prompt *****************************************************************

CREATE OR REPLACE VIEW TM_TABLE_COLUMNS AS
SELECT COLUMN_NAME.ROW_ID               AS ROW_ID
     , TABLE_ROW_ID.VALUE               AS TABLE_ROW_ID
     , COLUMN_NAME.VALUE                AS COLUMN_NAME
     , DATA_TYPE_ROW_ID.VALUE           AS DATA_TYPE_ROW_ID
     , PRIME.VALUE                      AS PRIME
     , DESCRIPTION.VALUE                AS DESCRIPTION
  FROM      TIMEMACHINE_STRINGS COLUMN_NAME
  LEFT JOIN TIMEMACHINE_NUMBERS TABLE_ROW_ID     ON ( COLUMN_NAME.TABLE_ID = TABLE_ROW_ID.TABLE_ID     AND COLUMN_NAME.ROW_ID = TABLE_ROW_ID.ROW_ID     AND TABLE_ROW_ID.COLUMN_ID     = 7 AND TM_NOW BETWEEN TABLE_ROW_ID.CREATED_AT     AND TABLE_ROW_ID.CLOSED_AT     )
  LEFT JOIN TIMEMACHINE_NUMBERS DATA_TYPE_ROW_ID ON ( COLUMN_NAME.TABLE_ID = DATA_TYPE_ROW_ID.TABLE_ID AND COLUMN_NAME.ROW_ID = DATA_TYPE_ROW_ID.ROW_ID AND DATA_TYPE_ROW_ID.COLUMN_ID = 8 AND TM_NOW BETWEEN DATA_TYPE_ROW_ID.CREATED_AT AND DATA_TYPE_ROW_ID.CLOSED_AT )
  LEFT JOIN TIMEMACHINE_STRINGS PRIME            ON ( COLUMN_NAME.TABLE_ID = PRIME.TABLE_ID            AND COLUMN_NAME.ROW_ID = PRIME.ROW_ID            AND PRIME.COLUMN_ID            = 9 AND TM_NOW BETWEEN PRIME.CREATED_AT            AND PRIME.CLOSED_AT            )
  LEFT JOIN TIMEMACHINE_STRINGS DESCRIPTION      ON ( COLUMN_NAME.TABLE_ID = DESCRIPTION.TABLE_ID      AND COLUMN_NAME.ROW_ID = DESCRIPTION.ROW_ID      AND DESCRIPTION.COLUMN_ID      =10 AND TM_NOW BETWEEN DESCRIPTION.CREATED_AT      AND DESCRIPTION.CLOSED_AT      )
 WHERE COLUMN_NAME.TABLE_ID  = 3 
   AND COLUMN_NAME.COLUMN_ID = 6 
   AND TM_NOW BETWEEN COLUMN_NAME.CREATED_AT AND COLUMN_NAME.CLOSED_AT 
;

-----------------------------------------------------------------------------------------------

CREATE OR REPLACE TRIGGER TRG_TM_TABLE_COLUMNS_IIR 
   INSTEAD OF INSERT ON TM_TABLE_COLUMNS FOR EACH ROW
DECLARE
    V_ROW_ID    NUMBER;
    V_NOW       TIMESTAMP := SYS_EXTRACT_UTC( SYSTIMESTAMP );
BEGIN
    IF PKG_TM_NOW.G_NOW IS NULL THEN
        V_ROW_ID := NVL( :NEW.ROW_ID, TIMEMACHINE_SEQUENCE.NEXTVAL );
        IF :NEW.COLUMN_NAME      IS NOT NULL THEN INSERT INTO TIMEMACHINE_STRINGS ( TABLE_ID, COLUMN_ID, ROW_ID, CREATED_AT, CLOSED_AT, VALUE, CREATED_BY ) VALUES ( 3, 6, V_ROW_ID, V_NOW, PKG_TM_NOW.G_FOREVER, :NEW.COLUMN_NAME, TM_TRANSACTION ); END IF;
        IF :NEW.TABLE_ROW_ID     IS NOT NULL THEN INSERT INTO TIMEMACHINE_NUMBERS ( TABLE_ID, COLUMN_ID, ROW_ID, CREATED_AT, CLOSED_AT, VALUE, CREATED_BY ) VALUES ( 3, 7, V_ROW_ID, V_NOW, PKG_TM_NOW.G_FOREVER, :NEW.TABLE_ROW_ID, TM_TRANSACTION ); END IF;
        IF :NEW.DATA_TYPE_ROW_ID IS NOT NULL THEN INSERT INTO TIMEMACHINE_NUMBERS ( TABLE_ID, COLUMN_ID, ROW_ID, CREATED_AT, CLOSED_AT, VALUE, CREATED_BY ) VALUES ( 3, 8, V_ROW_ID, V_NOW, PKG_TM_NOW.G_FOREVER, :NEW.DATA_TYPE_ROW_ID, TM_TRANSACTION ); END IF;
        IF :NEW.PRIME            IS NOT NULL THEN INSERT INTO TIMEMACHINE_STRINGS ( TABLE_ID, COLUMN_ID, ROW_ID, CREATED_AT, CLOSED_AT, VALUE, CREATED_BY ) VALUES ( 3, 9, V_ROW_ID, V_NOW, PKG_TM_NOW.G_FOREVER, :NEW.PRIME, TM_TRANSACTION ); END IF;
        IF :NEW.DESCRIPTION      IS NOT NULL THEN INSERT INTO TIMEMACHINE_STRINGS ( TABLE_ID, COLUMN_ID, ROW_ID, CREATED_AT, CLOSED_AT, VALUE, CREATED_BY ) VALUES ( 3,10, V_ROW_ID, V_NOW, PKG_TM_NOW.G_FOREVER, :NEW.DESCRIPTION , TM_TRANSACTION ); END IF;
    END IF;    
END;
/   

-----------------------------------------------------------------------------------------------

/*    TM_DATA_TYPES    */
INSERT INTO TM_TABLE_COLUMNS ( ROW_ID, TABLE_ROW_ID, COLUMN_NAME, DATA_TYPE_ROW_ID, DESCRIPTION, PRIME ) VALUES (  1, 1, 'DATA_TYPE_NAME' , 2, 'Name of data type', 'Y' );
INSERT INTO TM_TABLE_COLUMNS ( ROW_ID, TABLE_ROW_ID, COLUMN_NAME, DATA_TYPE_ROW_ID, DESCRIPTION        ) VALUES (  2, 1, 'DATA_TABLE_NAME', 2, 'The name of the physical table where this kind of data type is stored' );
INSERT INTO TM_TABLE_COLUMNS ( ROW_ID, TABLE_ROW_ID, COLUMN_NAME, DATA_TYPE_ROW_ID, DESCRIPTION        ) VALUES (  3, 1, 'DESCRIPTION'    , 2, 'Comment' );
COMMIT;

/*    TM_TABLE    */
INSERT INTO TM_TABLE_COLUMNS ( ROW_ID, TABLE_ROW_ID, COLUMN_NAME, DATA_TYPE_ROW_ID, DESCRIPTION, PRIME ) VALUES (  4, 2, 'TABLE_NAME'  , 2, 'Name of TM table', 'Y' );
INSERT INTO TM_TABLE_COLUMNS ( ROW_ID, TABLE_ROW_ID, COLUMN_NAME, DATA_TYPE_ROW_ID, DESCRIPTION        ) VALUES (  5, 2, 'DESCRIPTION' , 2, 'Comment' );
COMMIT;

/*    TM_TABLE_COLUMNS    */
INSERT INTO TM_TABLE_COLUMNS ( ROW_ID, TABLE_ROW_ID, COLUMN_NAME, DATA_TYPE_ROW_ID, DESCRIPTION, PRIME ) VALUES (  6, 3, 'COLUMN_NAME'      , 2, 'Name of TM table column', 'Y' );
INSERT INTO TM_TABLE_COLUMNS ( ROW_ID, TABLE_ROW_ID, COLUMN_NAME, DATA_TYPE_ROW_ID, DESCRIPTION        ) VALUES (  7, 3, 'TABLE_ROW_ID'     , 1, 'ID of TM table of this column' );
INSERT INTO TM_TABLE_COLUMNS ( ROW_ID, TABLE_ROW_ID, COLUMN_NAME, DATA_TYPE_ROW_ID, DESCRIPTION        ) VALUES (  8, 3, 'DATA_TYPE_ROW_ID' , 1, 'ID of TM type  of this column' );
INSERT INTO TM_TABLE_COLUMNS ( ROW_ID, TABLE_ROW_ID, COLUMN_NAME, DATA_TYPE_ROW_ID, DESCRIPTION        ) VALUES (  9, 3, 'PRIME'            , 2, 'PRIME = mandatory for ever = Element Primary key' );
INSERT INTO TM_TABLE_COLUMNS ( ROW_ID, TABLE_ROW_ID, COLUMN_NAME, DATA_TYPE_ROW_ID, DESCRIPTION        ) VALUES ( 10, 3, 'DESCRIPTION'      , 2, 'Comment' );
COMMIT;


Prompt *****************************************************************
Prompt **                     P R O G R A M S                         **
Prompt *****************************************************************

create or replace procedure TM_CREATE_TABLE ( I_TABLE_NAME in varchar2, I_DESCRIPTION in varchar2 default null ) as
    V_TABLE_ID    number;    
begin

    if PKG_TM_NOW.G_NOW is null then

        select min( ROW_ID )
          into V_TABLE_ID
          from TM_TABLES
         where upper( TABLE_NAME ) = upper( I_TABLE_NAME );
        
        if V_TABLE_ID is null then
        
            insert into TM_TABLES ( TABLE_NAME, DESCRIPTION ) values ( I_TABLE_NAME, I_DESCRIPTION );
        
        else
        
            update TM_TABLES
               set DESCRIPTION = I_DESCRIPTION
             where ROW_ID = V_TABLE_ID;         
        
        end if;

    end if;

end;
/

-----------------------------------------------------------------------------------------------

create or replace procedure TM_DROP_TABLE ( I_TABLE_NAME in varchar2 ) as
    V_TABLE_ID      number;    
    V_CNT           number := 0;
    V_INT           number;
begin

    if PKG_TM_NOW.G_NOW is null then

        select min( ROW_ID )
          into V_TABLE_ID
          from TM_TABLES
         where upper( TABLE_NAME ) = upper( I_TABLE_NAME );
        
        if V_TABLE_ID is not null then 
            -- check there is not any data 
            for L_R in ( select DATA_TABLE_NAME from TM_DATA_TYPES )
            loop
        
                execute immediate 'select nvl( ( select 1 from '||L_R.DATA_TABLE_NAME||' where TABLE_ID = '||to_char( V_TABLE_ID ) ||' and rownum < 2 ), 0 ) from dual' into V_INT;
                V_CNT := V_CNT + V_INT;
        
            end loop;
        
            if V_CNT = 0 then
        
                delete TM_TABLE_COLUMNS where TABLE_ROW_ID = V_TABLE_ID;         
                delete TM_TABLES        where ROW_ID       = V_TABLE_ID;         
                execute immediate 'drop view '||I_TABLE_NAME;
        
            else    
                RAISE_APPLICATION_ERROR( -20000, 'Table '||I_TABLE_NAME||' can not dropped because it contains data' );
            end if;
        
        end if;

    end if;
    
end;
/

-----------------------------------------------------------------------------------------------

create or replace procedure TM_CREATE_TABLE_COLUMN ( I_TABLE_NAME in varchar2, I_COLUMN_NAME in varchar2, I_DATA_TYPE_NAME in varchar2, I_PRIME in varchar2 default null, I_DESCRIPTION in varchar2 default null ) as
    V_TABLE_ID          number;    
    V_COLUMN_ID         number;    
    V_TYPE_ID           number;    
begin

    if PKG_TM_NOW.G_NOW is null then

        select min( ROW_ID )
          into V_TABLE_ID
          from TM_TABLES
         where upper( TABLE_NAME ) = upper( I_TABLE_NAME );
        
        select min( ROW_ID )
          into V_TYPE_ID
          from TM_DATA_TYPES
         where upper( DATA_TYPE_NAME ) = upper( I_DATA_TYPE_NAME );
        
        if V_TABLE_ID is not null and V_TYPE_ID is not null then
        
            select min( ROW_ID )
              into V_COLUMN_ID
              from TM_TABLE_COLUMNS
             where TABLE_ROW_ID         = V_TABLE_ID
               and upper( COLUMN_NAME ) = upper( I_COLUMN_NAME );
        
            if V_COLUMN_ID is null then
        
                insert into TM_TABLE_COLUMNS ( TABLE_ROW_ID, COLUMN_NAME, DATA_TYPE_ROW_ID, DESCRIPTION, PRIME ) VALUES ( V_TABLE_ID, I_COLUMN_NAME, V_TYPE_ID, I_DESCRIPTION, I_PRIME );
        
            else
        
                update TM_TABLE_COLUMNS
                   set DESCRIPTION = I_DESCRIPTION
                 where ROW_ID = V_COLUMN_ID;         
        
            end if;
        
        end if; 
        
    end if; 
end;
/

-----------------------------------------------------------------------------------------------

create or replace procedure TM_DROP_TABLE_COLUMN ( I_TABLE_NAME in varchar2, I_COLUMN_NAME in varchar2 ) as
    V_TABLE_ID          number;    
    V_COLUMN_ID         number;    
    V_CNT               number := 0;
    V_INT               number;
begin

    if PKG_TM_NOW.G_NOW is null then

        select min( ROW_ID )
          into V_TABLE_ID
          from TM_TABLES
         where upper( TABLE_NAME ) = upper( I_TABLE_NAME );
        
        if V_TABLE_ID is not null then 
        
            select min( ROW_ID )
              into V_COLUMN_ID
              from TM_TABLE_COLUMNS
             where TABLE_ROW_ID         = V_TABLE_ID
               and upper( COLUMN_NAME ) = upper( I_COLUMN_NAME );
        
            if V_COLUMN_ID is not null then
        
                -- check there is not any data 
                for L_R in ( select DATA_TABLE_NAME from TM_DATA_TYPES )
                loop
        
                    execute immediate 'select nvl( ( select 1 from '||L_R.DATA_TABLE_NAME||' where TABLE_ID = '||to_char( V_TABLE_ID ) ||' and COLUMN_ID = '||to_char( V_COLUMN_ID ) ||' and rownum < 2 ), 0 ) from dual' into V_INT;
                    V_CNT := V_CNT + V_INT;
        
                end loop;
        
                if V_CNT = 0 then
        
                    delete TM_TABLE_COLUMNS where TABLE_ROW_ID = V_TABLE_ID and ROW_ID = V_COLUMN_ID;         
        
                else    
                    RAISE_APPLICATION_ERROR( -20001, 'Table column'||I_TABLE_NAME||','||I_COLUMN_NAME||' can not dropped because it contains data' );
                end if;
        
            end if;
        
        end if;

    end if;
end;
/

-----------------------------------------------------------------------------------------------

create or replace function TM_CREATE_VIEW_SQL ( I_TABLE_NAME in varchar2 ) return varchar2 as
    V_HEAD              varchar2(  1000 );
    V_SELECT            varchar2( 32000 );
    V_FROM              varchar2( 32000 );
    V_WHERE             varchar2(  1000 );
    V_TABLE_ID          number;    
    V_COLUMN_ID         number;    
    V_COLUMN_NAME       varchar2(   300 );    
begin

    if PKG_TM_NOW.G_NOW is null then

        select min( ROW_ID )
          into V_TABLE_ID
          from TM_TABLES
         where upper( TABLE_NAME ) = upper( I_TABLE_NAME );
        
        if V_TABLE_ID is not null then 
        
            select min( COLUMN_NAME ), min( ROW_ID ) 
              into V_COLUMN_NAME, V_COLUMN_ID
              from TM_TABLE_COLUMNS
             where TABLE_ROW_ID   = V_TABLE_ID
               and upper( PRIME ) = 'Y';
        
            if V_COLUMN_ID is not null then
        
                V_HEAD := 'CREATE OR REPLACE VIEW '||I_TABLE_NAME||' AS /* created '||to_char(SYS_EXTRACT_UTC( SYSTIMESTAMP ),'yyyy.mm.dd hh24:mi:ss')||' '||TM_TRANSACTION||' */'||PKG_TM.G_CRLF;
                for L_R in ( select ROW_ID
                                  , COLUMN_NAME
                                  , DATA_TYPE_ROW_ID
                                  , ( select DATA_TYPE_NAME  from TM_DATA_TYPES where ROW_ID = DATA_TYPE_ROW_ID ) as DATA_TYPE_NAME
                                  , ( select DATA_TABLE_NAME from TM_DATA_TYPES where ROW_ID = DATA_TYPE_ROW_ID ) as DATA_TABLE_NAME
                                  , TABLE_ROW_ID
                                  , ( select TABLE_NAME from TM_TABLES where ROW_ID = TABLE_ROW_ID ) as TABLE_NAME
                               from TM_TABLE_COLUMNS
                              where TABLE_ROW_ID = V_TABLE_ID
                                and ROW_ID       = V_COLUMN_ID            
                )
                loop
                    V_SELECT := ' SELECT '||V_COLUMN_NAME||'.ROW_ID AS ROW_ID, '||V_COLUMN_NAME||'.VALUE AS '||V_COLUMN_NAME||PKG_TM.G_CRLF;
                    V_FROM   := ' FROM '||L_R.DATA_TABLE_NAME||' '||V_COLUMN_NAME||PKG_TM.G_CRLF;
                end loop;
        
                
                for L_R in ( select ROW_ID
                                  , COLUMN_NAME
                                  , DATA_TYPE_ROW_ID
                                  , ( select DATA_TYPE_NAME  from TM_DATA_TYPES where ROW_ID = DATA_TYPE_ROW_ID ) as DATA_TYPE_NAME
                                  , ( select DATA_TABLE_NAME from TM_DATA_TYPES where ROW_ID = DATA_TYPE_ROW_ID ) as DATA_TABLE_NAME
                                  , TABLE_ROW_ID
                                  , ( select TABLE_NAME from TM_TABLES where ROW_ID = TABLE_ROW_ID ) as TABLE_NAME
                               from TM_TABLE_COLUMNS
                              where TABLE_ROW_ID = V_TABLE_ID
                                and ROW_ID       != V_COLUMN_ID
                              order by ROW_ID  
                )
                loop
                    V_SELECT := V_SELECT||' , '||L_R.COLUMN_NAME||'.VALUE AS '||L_R.COLUMN_NAME||PKG_TM.G_CRLF;
                    V_FROM   := V_FROM  ||' LEFT JOIN '||L_R.DATA_TABLE_NAME||' '||L_R.COLUMN_NAME||' ON ( '||V_COLUMN_NAME||'.TABLE_ID = '||L_R.COLUMN_NAME||'.TABLE_ID AND '||V_COLUMN_NAME||'.ROW_ID = '||L_R.COLUMN_NAME||'.ROW_ID AND ';
                    V_FROM   := V_FROM  ||L_R.COLUMN_NAME||'.COLUMN_ID = '||L_R.ROW_ID||' AND TM_NOW BETWEEN '||L_R.COLUMN_NAME||'.CREATED_AT AND '||L_R.COLUMN_NAME||'.CLOSED_AT ) '||PKG_TM.G_CRLF;
                end loop;
        
                V_WHERE := ' WHERE '||V_COLUMN_NAME||'.TABLE_ID = '||to_char(V_TABLE_ID)||' AND '||V_COLUMN_NAME||'.COLUMN_ID = '||to_char(V_COLUMN_ID)||' AND TM_NOW BETWEEN '||V_COLUMN_NAME||'.CREATED_AT AND '||V_COLUMN_NAME||'.CLOSED_AT '||PKG_TM.G_CRLF;
        
                return V_HEAD||V_SELECT||V_FROM||V_WHERE;
                
            end if;
            
        end if;

    end if;

    return null;

end;
/
-----------------------------------------------------------------------------------------------

create or replace procedure TM_CREATE_VIEW ( I_TABLE_NAME in varchar2 ) as
begin
    execute immediate TM_CREATE_VIEW_SQL( I_TABLE_NAME );
end;
/

-----------------------------------------------------------------------------------------------

create or replace function TM_CREATE_INSERT_TRIGGER_SQL ( I_TABLE_NAME in varchar2 ) return varchar2 as
    V_SQL               varchar2( 32767 BYTE );
    V_TABLE_ID          number;    
begin

    if PKG_TM_NOW.G_NOW is null then

        select min( ROW_ID )
          into V_TABLE_ID
          from TM_TABLES
         where upper( TABLE_NAME ) = upper( I_TABLE_NAME );

        if V_TABLE_ID is not null then 

            V_SQL := 'CREATE OR REPLACE TRIGGER TRG_'||I_TABLE_NAME||'_IIR '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' INSTEAD OF INSERT ON '||I_TABLE_NAME||' FOR EACH ROW /* created '||to_char(SYS_EXTRACT_UTC( SYSTIMESTAMP ),'yyyy.mm.dd hh24:mi:ss')||' '||TM_TRANSACTION||' */'||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' DECLARE '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' V_ROW_ID    NUMBER; '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' V_NOW       TIMESTAMP := SYS_EXTRACT_UTC( SYSTIMESTAMP ); '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' BEGIN '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' IF PKG_TM_NOW.G_NOW IS NULL THEN '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||'     V_ROW_ID := NVL( :NEW.ROW_ID, TIMEMACHINE_SEQUENCE.NEXTVAL ); '||PKG_TM.G_CRLF;
           
            for L_R in ( select ROW_ID
                              , COLUMN_NAME
                              , DATA_TYPE_ROW_ID
                              , ( select DATA_TYPE_NAME  from TM_DATA_TYPES where ROW_ID = DATA_TYPE_ROW_ID ) as DATA_TYPE_NAME
                              , ( select DATA_TABLE_NAME from TM_DATA_TYPES where ROW_ID = DATA_TYPE_ROW_ID ) as DATA_TABLE_NAME
                              , TABLE_ROW_ID
                              , ( select TABLE_NAME from TM_TABLES where ROW_ID = TABLE_ROW_ID ) as TABLE_NAME
                           from TM_TABLE_COLUMNS
                          where TABLE_ROW_ID = V_TABLE_ID
                          order by ROW_ID
            )
            loop
           
                V_SQL := V_SQL ||' IF :NEW.'||L_R.COLUMN_NAME||' IS NOT NULL THEN INSERT INTO '||L_R.DATA_TABLE_NAME||' ( TABLE_ID, COLUMN_ID, ROW_ID, CREATED_AT, CLOSED_AT, VALUE, CREATED_BY ) VALUES ( ';
                V_SQL := V_SQL ||to_char( V_TABLE_ID )||','||to_char( L_R.ROW_ID )||', V_ROW_ID, V_NOW, PKG_TM_NOW.G_FOREVER, :NEW.'||L_R.COLUMN_NAME||', TM_TRANSACTION ); END IF; '||PKG_TM.G_CRLF;
           
            end loop;
           
            V_SQL := V_SQL ||' END IF; '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' END; '||PKG_TM.G_CRLF;
           
           
            return V_SQL;
            
        end if;
                    
    end if;

    return null;

end;
/

-----------------------------------------------------------------------------------------------

create or replace procedure TM_CREATE_INSERT_TRIGGER ( I_TABLE_NAME in varchar2 ) as
begin
    execute immediate TM_CREATE_INSERT_TRIGGER_SQL( I_TABLE_NAME );
end;
/

-----------------------------------------------------------------------------------------------

create or replace function TM_CREATE_DELETE_TRIGGER_SQL ( I_TABLE_NAME in varchar2 ) return varchar2 as
    V_SQL               varchar2( 32767 BYTE );
    V_TABLE_ID          number;    
begin

    if PKG_TM_NOW.G_NOW is null then

        select min( ROW_ID )
          into V_TABLE_ID
          from TM_TABLES
         where upper( TABLE_NAME ) = upper( I_TABLE_NAME );

        if V_TABLE_ID is not null then 

            V_SQL := 'CREATE OR REPLACE TRIGGER TRG_'||I_TABLE_NAME||'_IDR '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' INSTEAD OF DELETE ON '||I_TABLE_NAME||' FOR EACH ROW /* created '||to_char(SYS_EXTRACT_UTC( SYSTIMESTAMP ),'yyyy.mm.dd hh24:mi:ss')||' '||TM_TRANSACTION||' */'||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' DECLARE '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' V_NOW TIMESTAMP := SYS_EXTRACT_UTC( SYSTIMESTAMP ); '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' BEGIN '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' IF PKG_TM_NOW.G_NOW IS NULL THEN '||PKG_TM.G_CRLF;
           
            for L_R in ( select ROW_ID
                              , COLUMN_NAME
                              , DATA_TYPE_ROW_ID
                              , ( select DATA_TYPE_NAME  from TM_DATA_TYPES where ROW_ID = DATA_TYPE_ROW_ID ) as DATA_TYPE_NAME
                              , ( select DATA_TABLE_NAME from TM_DATA_TYPES where ROW_ID = DATA_TYPE_ROW_ID ) as DATA_TABLE_NAME
                              , TABLE_ROW_ID
                              , ( select TABLE_NAME from TM_TABLES where ROW_ID = TABLE_ROW_ID ) as TABLE_NAME
                           from TM_TABLE_COLUMNS
                          where TABLE_ROW_ID = V_TABLE_ID
                          order by ROW_ID
            )
            loop
           
                V_SQL := V_SQL ||' UPDATE '||L_R.DATA_TABLE_NAME||' SET CLOSED_AT = V_NOW, CLOSED_BY = TM_TRANSACTION WHERE TM_NOW BETWEEN CREATED_AT AND CLOSED_AT AND TABLE_ID = '||to_char( V_TABLE_ID )||' AND COLUMN_ID = '||to_char( L_R.ROW_ID )||' AND ROW_ID = :OLD.ROW_ID;'||PKG_TM.G_CRLF;
           
            end loop;
           
            V_SQL := V_SQL ||' END IF; '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' END; '||PKG_TM.G_CRLF;
           
           
            return V_SQL;
            
        end if;
                    
    end if;

    return null;

end;
/

-----------------------------------------------------------------------------------------------

create or replace procedure TM_CREATE_DELETE_TRIGGER ( I_TABLE_NAME in varchar2 ) as
begin
    execute immediate TM_CREATE_DELETE_TRIGGER_SQL( I_TABLE_NAME );
end;
/

-----------------------------------------------------------------------------------------------

create or replace function TM_CREATE_UPDATE_TRIGGER_SQL ( I_TABLE_NAME in varchar2 ) return varchar2 as
    V_SQL               varchar2( 32767 BYTE );
    V_TABLE_ID          number;    
begin

    if PKG_TM_NOW.G_NOW is null then

        select min( ROW_ID )
          into V_TABLE_ID
          from TM_TABLES
         where upper( TABLE_NAME ) = upper( I_TABLE_NAME );

        if V_TABLE_ID is not null then 

            V_SQL := 'CREATE OR REPLACE TRIGGER TRG_'||I_TABLE_NAME||'_IUR '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' INSTEAD OF UPDATE ON '||I_TABLE_NAME||' FOR EACH ROW /* created '||to_char(SYS_EXTRACT_UTC( SYSTIMESTAMP ),'yyyy.mm.dd hh24:mi:ss')||' '||TM_TRANSACTION||' */'||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' DECLARE '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' V_NOW  TIMESTAMP := SYS_EXTRACT_UTC( SYSTIMESTAMP ); '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' BEGIN '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' IF PKG_TM_NOW.G_NOW IS NULL THEN '||PKG_TM.G_CRLF;
           
            for L_R in ( select ROW_ID
                              , COLUMN_NAME
                              , DATA_TYPE_ROW_ID
                              , ( select DATA_TYPE_NAME  from TM_DATA_TYPES where ROW_ID = DATA_TYPE_ROW_ID ) as DATA_TYPE_NAME
                              , ( select DATA_TABLE_NAME from TM_DATA_TYPES where ROW_ID = DATA_TYPE_ROW_ID ) as DATA_TABLE_NAME
                              , TABLE_ROW_ID
                              , ( select TABLE_NAME from TM_TABLES where ROW_ID = TABLE_ROW_ID ) as TABLE_NAME
                           from TM_TABLE_COLUMNS
                          where TABLE_ROW_ID = V_TABLE_ID
                          order by ROW_ID
            )
            loop
           
                V_SQL := V_SQL ||'IF PKG_DIFF.VALUES_ARE_DIFFER(:OLD.'||L_R.COLUMN_NAME||',:NEW.'||L_R.COLUMN_NAME||') THEN'||PKG_TM.G_CRLF;
                V_SQL := V_SQL ||'UPDATE '||L_R.DATA_TABLE_NAME||' SET CLOSED_AT = V_NOW, CLOSED_BY = TM_TRANSACTION WHERE TM_NOW BETWEEN CREATED_AT AND CLOSED_AT AND TABLE_ID = '||to_char( V_TABLE_ID )||' AND COLUMN_ID = '||to_char( L_R.ROW_ID )||' AND ROW_ID = :OLD.ROW_ID;'||PKG_TM.G_CRLF;
                V_SQL := V_SQL ||'IF :NEW.'||L_R.COLUMN_NAME||' IS NOT NULL THEN INSERT INTO '||L_R.DATA_TABLE_NAME||' (TABLE_ID,COLUMN_ID,ROW_ID,CREATED_AT,CLOSED_AT,VALUE,CREATED_BY) VALUES (';
                V_SQL := V_SQL ||to_char( V_TABLE_ID )||','||to_char( L_R.ROW_ID )||',:NEW.ROW_ID,V_NOW,PKG_TM_NOW.G_FOREVER,:NEW.'||L_R.COLUMN_NAME||',TM_TRANSACTION);END IF;'||PKG_TM.G_CRLF;
                V_SQL := V_SQL ||'END IF;'||PKG_TM.G_CRLF;
           
            end loop;
           
            V_SQL := V_SQL ||' END IF; '||PKG_TM.G_CRLF;
            V_SQL := V_SQL ||' END; '||PKG_TM.G_CRLF;
           
           
            return V_SQL;
            
        end if;
                    
    end if;

    return null;

end;
/

-----------------------------------------------------------------------------------------------

create or replace procedure TM_CREATE_UPDATE_TRIGGER ( I_TABLE_NAME in varchar2 ) as
begin
    execute immediate TM_CREATE_UPDATE_TRIGGER_SQL( I_TABLE_NAME );
end;
/

-----------------------------------------------------------------------------------------------

create or replace procedure TM_CREATE_TRIGGERS ( I_TABLE_NAME in varchar2 ) as
begin
    TM_CREATE_INSERT_TRIGGER ( I_TABLE_NAME );
    TM_CREATE_DELETE_TRIGGER ( I_TABLE_NAME );
    TM_CREATE_UPDATE_TRIGGER ( I_TABLE_NAME );
end;
/

-----------------------------------------------------------------------------------------------

create or replace procedure TM_CREATE_OBJECTS ( I_TABLE_NAME in varchar2 ) as
begin
    for L_R in ( select * from TM_TABLES where upper( TABLE_NAME ) = upper( nvl( I_TABLE_NAME, TABLE_NAME ) ) )
    loop
        TM_CREATE_VIEW    ( L_R.TABLE_NAME );
        TM_CREATE_TRIGGERS( L_R.TABLE_NAME );        
    end loop;
end;
/

-----------------------------------------------------------------------------------------------

begin
    TM_CREATE_OBJECTS( 'TM_DATA_TYPES'    );
    TM_CREATE_OBJECTS( 'TM_TABLES'        );
    TM_CREATE_OBJECTS( 'TM_TABLE_COLUMNS' );
end;
/
    
-----------------------------------------------------------------------------------------------
