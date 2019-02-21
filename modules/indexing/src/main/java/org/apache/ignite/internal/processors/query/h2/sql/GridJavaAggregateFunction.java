package org.apache.ignite.internal.processors.query.h2.sql;

import org.h2.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class GridJavaAggregateFunction extends  GridSqlElement{

    private final String functionName;

    public GridJavaAggregateFunction(String functionName) {
        super(new ArrayList<GridSqlAst>());
        this.functionName = functionName;
    }

    public String reducerFunction(){
        return this.functionName + "Reducer";
    }

    public String getFunction(){
        return this.functionName;
    }

    @Override
    public String getSQL() {
        List<String> childSql = new ArrayList<>(size());

        for(int i =0;i<size();i++){
            childSql.add(child(i).getSQL());
        }

        return functionName + StringUtils.enclose(String.join(",", childSql));
    }
}
