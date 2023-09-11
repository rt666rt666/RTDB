package com.rt.rtdb.backend.parser;

import com.rt.rtdb.backend.parser.statement.*;
import com.rt.rtdb.common.Error;
import java.util.ArrayList;
import java.util.List;
/**
 * SQL 解析器
 * @author ryh
 * @version 1.0
 * @since 1.0
 * @create 2023/7/31 15:11
 **/
/**
 * 解析器类，将字节数组解析为命令对象。
 */
public class Parser {

    /**
     * 解析字节数组并返回相应的命令对象。
     *
     * @param statement 待解析的字节数组
     * @return 解析后的命令对象
     * @throws Exception 如果解析过程中发生异常
     */
    public static Object Parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek();
        tokenizer.pop();

        Object stat = null;
        Exception statErr = null;
        try {
            switch (token) {
                case "begin":
                    stat = parseBegin(tokenizer);  // 解析 BEGIN 命令
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);  // 解析 COMMIT 命令
                    break;
                case "abort":
                    stat = parseAbort(tokenizer);  // 解析 ABORT 命令
                    break;
                case "create":
                    stat = parseCreate(tokenizer);  // 解析 CREATE 命令
                    break;
                case "drop":
                    stat = parseDrop(tokenizer);  // 解析 DROP 命令
                    break;
                case "select":
                    stat = parseSelect(tokenizer);  // 解析 SELECT 命令
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);  // 解析 INSERT 命令
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);  // 解析 DELETE 命令
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);  // 解析 UPDATE 命令
                    break;
                case "show":
                    stat = parseShow(tokenizer);  // 解析 SHOW 命令
                    break;
                default:
                    throw Error.InvalidCommandException;  // 抛出无效命令异常
            }
        } catch (Exception e) {
            statErr = e;
        }
        try {
            String next = tokenizer.peek();
            if (!"".equals(next)) {
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch (Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }
        if (statErr != null) {
            throw statErr;
        }
        return stat;
    }

    /**
     * 解析 SHOW 命令。
     *
     * @param tokenizer Tokenizer 对象
     * @return 解析后的 Show 对象
     * @throws Exception 如果解析过程中发生异常
     */
    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            return new Show();
        }
        throw Error.InvalidCommandException;  // 抛出无效命令异常
    }

    /**
     * 解析 UPDATE 命令。
     *
     * @param tokenizer Tokenizer 对象
     * @return 解析后的 Update 对象
     * @throws Exception 如果解析过程中发生异常
     */
    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();

        update.tableName = tokenizer.peek();  // 获取表名
        tokenizer.pop();

        if (!"set".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;  // 抛出无效命令异常
        }
        tokenizer.pop();

        update.fieldName = tokenizer.peek();  // 获取字段名
        tokenizer.pop();

        if (!"=".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;  // 抛出无效命令异常
        }
        tokenizer.pop();

        update.value = tokenizer.peek();  // 获取字段值
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            update.where = null;
            return update;
        }

        update.where = parseWhere(tokenizer);  // 解析 WHERE 子句
        return update;
    }

    /**
     * 解析 DELETE 命令。
     *
     * @param tokenizer Tokenizer 对象
     * @return 解析后的 Delete 对象
     * @throws Exception 如果解析过程中发生异常
     */
    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();

        if (!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;  // 抛出无效命令异常
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;  // 抛出无效命令异常
        }

        delete.tableName = tableName;
        tokenizer.pop();

        delete.where = parseWhere(tokenizer);  // 解析 WHERE 子句

        return delete;
    }

    /**
     * 解析 INSERT 命令。
     *
     * @param tokenizer Tokenizer 对象
     * @return 解析后的 Insert 对象
     * @throws Exception 如果解析过程中发生异常
     */
    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();

        if (!"into".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;  // 抛出无效命令异常
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.InvalidCommandException;  // 抛出无效命令异常
        }

        insert.tableName = tableName;
        tokenizer.pop();

        if (!"values".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;  // 抛出无效命令异常
        }

        List<String> values = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if ("".equals(value)) {
                break;
            } else {
                values.add(value);
            }
        }

        insert.values = values.toArray(new String[values.size()]);

        return insert;
    }


    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();

        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        if("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            while(true) {
                String field = tokenizer.peek();
                if(!isName(field)) {
                    throw Error.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                if(",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        read.fields = fields.toArray(new String[fields.size()]);

        if(!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        read.tableName = tableName;
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            read.where = null;
            return read;
        }

        read.where = parseWhere(tokenizer);
        return read;
    }

    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        if(!"where".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;

        String logicOp = tokenizer.peek();
        if("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }
        if(!isLogicOp(logicOp)) {
            throw Error.InvalidCommandException;
        }
        where.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;

        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return where;
    }

    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();
        
        String field = tokenizer.peek();
        if(!isName(field)) {
            throw Error.InvalidCommandException;
        }
        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if(!isCmpOp(op)) {
            throw Error.InvalidCommandException;
        }
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }

    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        
        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    private static Create parseCreate(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        Create create = new Create();
        String name = tokenizer.peek();
        if(!isName(name)) {
            throw Error.InvalidCommandException;
        }
        create.tableName = name;

        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if("(".equals(field)) {
                break;
            }

            if(!isName(field)) {
                throw Error.InvalidCommandException;
            }

            tokenizer.pop();
            String fieldType = tokenizer.peek();
            if(!isType(fieldType)) {
                throw Error.InvalidCommandException;
            }
            fNames.add(field);
            fTypes.add(fieldType);
            tokenizer.pop();
            
            String next = tokenizer.peek();
            if(",".equals(next)) {
                continue;
            } else if("".equals(next)) {
                throw Error.TableNoIndexException;
            } else if("(".equals(next)) {
                break;
            } else {
                throw Error.InvalidCommandException;
            }
        }
        create.fieldName = fNames.toArray(new String[fNames.size()]);
        create.fieldType = fTypes.toArray(new String[fTypes.size()]);

        tokenizer.pop();
        if(!"index".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        List<String> indexes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if(")".equals(field)) {
                break;
            }
            if(!isName(field)) {
                throw Error.InvalidCommandException;
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[indexes.size()]);
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return create;
    }

    private static boolean isType(String tp) {
        return ("int32".equals(tp) || "int64".equals(tp) ||
        "string".equals(tp));
    }

    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return new Abort();
    }

    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return new Commit();
    }

    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek();
        Begin begin = new Begin();
        if("".equals(isolation)) {
            return begin;
        }
        if(!"isolation".equals(isolation)) {
            throw Error.InvalidCommandException;
        }

        tokenizer.pop();
        String level = tokenizer.peek();
        if(!"level".equals(level)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tmp1 = tokenizer.peek();
        if("read".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("committed".equals(tmp2)) {
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else if("repeatable".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("read".equals(tmp2)) {
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else {
            throw Error.InvalidCommandException;
        }
    }

    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }
}
