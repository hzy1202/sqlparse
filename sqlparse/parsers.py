# -*- coding: utf-8 -*-
import abc

from sqlparse import engine
from sqlparse import filters
from sqlparse import lexer
from sqlparse import tokens as T
from sqlparse.engine import grouping
from sqlparse.engine.filter import StatementFilter


class SQLParser(object):

    dialect = 'unknown'

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def parse(self, sql, encoding):
        raise NotImplementedError()


def _split_statements(stream):
    splitter = StatementFilter()
    return list(splitter.process(None, stream))


def _is_create_table_statement(statement):
    if statement.get_type() == 'CREATE':
        first_keyword_token = statement.token_first()
        first_keyword_token_index = statement.token_index(first_keyword_token)
        next_token, token_index = _get_token_and_index(
            statement,
            token_type=T.Keyword,
            start_index=first_keyword_token_index + 1
        )
        if _is_keyword_token(next_token, 'TABLE'):
            return True

        if _is_keyword_token(next_token, 'TEMPORARY'):
            next_token, token_index = _get_token_and_index(
                statement,
                token_type=T.Keyword,
                start_index=token_index + 1
            )
            if _is_keyword_token(next_token, 'TABLE'):
                return True
    return False


def _get_token_and_index(stmt, token_type, start_index):
    next_token = stmt.token_next_by_type(start_index, token_type)
    token_index = stmt.token_index(next_token)
    return next_token, token_index


def _is_keyword_token(token, normalized_keyword):
    return token and token.normalized == normalized_keyword


class GeneralSQLParser(SQLParser):

    dialect = None

    def parse(self, sql, encoding):
        stream = lexer.tokenize(sql, encoding)
        statements = _split_statements(stream)
        stack = engine.FilterStack()
        stack.enable_grouping()
        for statement in statements:
            yield stack.run(statement)


class MysqlSQLParser(SQLParser):

    dialect = 'mysql'

    def parse(self, sql, encoding):
        stream = lexer.tokenize(sql, encoding)
        statements = _split_statements(stream)

        default_stack = engine.FilterStack()
        default_stack.enable_grouping()
        create_table_statement_filter_stack = engine.FilterStack(
            stmtprocess=[filters.MysqlCreateStatementFilter()],
            grouping_funcs=[grouping.group_brackets]
        )
        create_table_statement_filter_stack.enable_grouping()
        for statement in statements:
            if _is_create_table_statement(statement):
                yield create_table_statement_filter_stack.run(statement)
            else:
                yield default_stack.run(statement)
