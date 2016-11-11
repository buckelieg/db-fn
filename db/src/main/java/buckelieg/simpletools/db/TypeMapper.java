package buckelieg.simpletools.db;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.SQLType;

@ParametersAreNonnullByDefault
public interface TypeMapper { // TODO needed for stored procedures as no automatic conversion is provided for them. At least by Derby

    @Nonnull SQLType toSQLType(Class<?> javaType);

    @Nonnull <T extends SQLType> Class<?> toJavaType(T type);

}
