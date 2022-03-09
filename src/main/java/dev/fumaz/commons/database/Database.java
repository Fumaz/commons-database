package dev.fumaz.commons.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Database {

    private final HikariDataSource source;
    private final ExecutorService executor;

    public Database(HikariConfig config) {
        this.source = new HikariDataSource(config);
        this.executor = Executors.newCachedThreadPool();
    }

    public void useConnection(SQLConsumer<Connection> consumer) {
        try (Connection connection = getConnection()) {
            consumer.accept(connection);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public void useConnectionAsynchronously(SQLConsumer<Connection> consumer) {
        executor.submit(() -> useConnection(consumer));
    }

    public void execute(String query, SQLConsumer<PreparedStatement> consumer) {
        try (Connection connection = getConnection(); PreparedStatement statement = connection.prepareStatement(query)) {
            consumer.accept(statement);
            statement.execute();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public void executeAsynchronously(String query, SQLConsumer<PreparedStatement> consumer) {
        executor.submit(() -> execute(query, consumer));
    }

    public void getResult(String query, SQLConsumer<PreparedStatement> statementSQLConsumer, SQLConsumer<ResultSet> resultSetSQLConsumer) {
        try (Connection connection = getConnection(); PreparedStatement statement = connection.prepareStatement(query)) {
            statementSQLConsumer.accept(statement);
            ResultSet resultSet = statement.executeQuery();
            resultSetSQLConsumer.accept(resultSet);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public void getResultAsynchronously(String query, SQLConsumer<PreparedStatement> statementSQLConsumer, SQLConsumer<ResultSet> resultSetSQLConsumer) {
        executor.submit(() -> getResult(query, statementSQLConsumer, resultSetSQLConsumer));
    }

    public void getResult(String query, SQLConsumer<ResultSet> consumer) {
        getResult(query, (s) -> {}, consumer);
    }

    public void getResultAsynchronously(String query, SQLConsumer<ResultSet> consumer) {
        getResultAsynchronously(query, (s) -> {}, consumer);
    }

    protected Connection getConnection() throws SQLException {
        return source.getConnection();
    }

}
