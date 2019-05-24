package org.apache.ignite.internal.processors.query.h2.opt;

import org.h2.engine.SysProperties;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.util.StatementBuilder;
import org.h2.value.CompareMode;
import org.h2.value.Value;

import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.util.ArrayList;

public class NewValueArray extends Value {
    private final Class<?> componentType;
    private final Value[] values;
    private int hash;

    private NewValueArray(Class<?> var1, Value[] var2) {
        this.componentType = var1;
        this.values = var2;
    }

    private NewValueArray(Value[] var1) {
        this(Object.class, var1);
    }

    public static NewValueArray get(Value[] var0) {
        return new NewValueArray(var0);
    }

    public static NewValueArray get(Class<?> var0, Value[] var1) {
        return new NewValueArray(var0, var1);
    }

    public int hashCode() {
        if (this.hash != 0) {
            return this.hash;
        } else {
            int var1 = 1;
            Value[] var2 = this.values;
            int var3 = var2.length;

            for(int var4 = 0; var4 < var3; ++var4) {
                Value var5 = var2[var4];
                var1 = var1 * 31 + var5.hashCode();
            }

            this.hash = var1;
            return var1;
        }
    }

    public Value[] getList() {
        return this.values;
    }

    public int getType() {
        return 17;
    }

    public Class<?> getComponentType() {
        return this.componentType;
    }

    public long getPrecision() {
        long var1 = 0L;
        Value[] var3 = this.values;
        int var4 = var3.length;

        for(int var5 = 0; var5 < var4; ++var5) {
            Value var6 = var3[var5];
            var1 += var6.getPrecision();
        }

        return var1;
    }

    public String getString() {
        StatementBuilder var1 = new StatementBuilder("(");
        Value[] var2 = this.values;
        int var3 = var2.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            Value var5 = var2[var4];
            var1.appendExceptFirst(", ");
            var1.append(var5.getString());
        }

        return var1.append(')').toString();
    }

    protected int compareSecure(Value var1, CompareMode var2) {
        NewValueArray var3 = (NewValueArray)var1;
        if (this.values == var3.values) {
            return 0;
        } else {
            int var4 = this.values.length;
            int var5 = var3.values.length;
            int var6 = Math.min(var4, var5);

            for(int var7 = 0; var7 < var6; ++var7) {
                Value var8 = this.values[var7];
                Value var9 = var3.values[var7];
                int var10 = var8.compareTo(var9, var2);
                if (var10 != 0) {
                    return var10;
                }
            }

            return Integer.compare(var4, var5);
        }
    }

    public Object getObject() {
        int var1 = this.values.length;
        Object var2 = Array.newInstance(this.componentType, var1);

        for(int var3 = 0; var3 < var1; ++var3) {
            Value var4 = this.values[var3];
            if (!SysProperties.OLD_RESULT_SET_GET_OBJECT) {
                int var5 = var4.getType();
                if (var5 == 2 || var5 == 3) {
                    Array.set(var2,var3, var4.getInt());
                    continue;
                }
            }

            Array.set(var2,var3, var4.getObject());
        }

        return var2;
    }

    public void set(PreparedStatement var1, int var2) {
        throw this.throwUnsupportedExceptionForType("PreparedStatement.set");
    }

    public String getSQL() {
        StatementBuilder var1 = new StatementBuilder("(");
        Value[] var2 = this.values;
        int var3 = var2.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            Value var5 = var2[var4];
            var1.appendExceptFirst(", ");
            var1.append(var5.getSQL());
        }

        if (this.values.length == 1) {
            var1.append(',');
        }

        return var1.append(')').toString();
    }

    public String getTraceSQL() {
        StatementBuilder var1 = new StatementBuilder("(");
        Value[] var2 = this.values;
        int var3 = var2.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            Value var5 = var2[var4];
            var1.appendExceptFirst(", ");
            var1.append(var5 == null ? "null" : var5.getTraceSQL());
        }

        return var1.append(')').toString();
    }

    public int getDisplaySize() {
        long var1 = 0L;
        Value[] var3 = this.values;
        int var4 = var3.length;

        for(int var5 = 0; var5 < var4; ++var5) {
            Value var6 = var3[var5];
            var1 += (long)var6.getDisplaySize();
        }

        return MathUtils.convertLongToInt(var1);
    }

    public boolean equals(Object var1) {
        if (!(var1 instanceof NewValueArray)) {
            return false;
        } else {
            NewValueArray var2 = (NewValueArray)var1;
            if (this.values == var2.values) {
                return true;
            } else {
                int var3 = this.values.length;
                if (var3 != var2.values.length) {
                    return false;
                } else {
                    for(int var4 = 0; var4 < var3; ++var4) {
                        if (!this.values[var4].equals(var2.values[var4])) {
                            return false;
                        }
                    }

                    return true;
                }
            }
        }
    }

    public int getMemory() {
        int var1 = 32;
        Value[] var2 = this.values;
        int var3 = var2.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            Value var5 = var2[var4];
            var1 += var5.getMemory() + 8;
        }

        return var1;
    }

    public Value convertPrecision(long var1, boolean var3) {
        if (!var3) {
            return this;
        } else {
            ArrayList var4 = New.arrayList();
            Value[] var5 = this.values;
            int var6 = var5.length;

            for(int var7 = 0; var7 < var6; ++var7) {
                Value var8 = var5[var7];
                var8 = var8.convertPrecision(var1, true);
                var1 -= Math.max(1L, var8.getPrecision());
                if (var1 < 0L) {
                    break;
                }

                var4.add(var8);
            }

            return get((Value[])var4.toArray(new Value[0]));
        }
    }
}
