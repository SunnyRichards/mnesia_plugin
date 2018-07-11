%%%-------------------------------------------------------------------
%%% @author sunnyrichards
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Jul 2018 7:41 PM
%%%-------------------------------------------------------------------
-module(mnesia_util).
-author("sunnyrichardss").

%% API

-export([check_n_write/4,check_n_write_index/5,multiple_write/2]).
-export([search/2,delete/2,update/3,index_search/3,all_keys/1,replace_key/4]).
-export([check_n_write_multiple_tab/5,write/3,delete_n_return/2,multiple_read/2]).

%%======================================================================================================================
%% @doc - write
%% Handles all create operations to the database.
%%======================================================================================================================

write(TableName,Record,LockKind)->
  Fun= fun()-> mnesia:write(TableName,Record,LockKind) end,
  mnesia:transaction(Fun).

%%======================================================================================================================
%% @doc - check_n_write
%% Handles all safe create - (checks db before writing) operations to the database.
%%======================================================================================================================

check_n_write(TableName,Record,LockKind,PrimaryKey)->
  Fun= fun()->
    case mnesia:read(TableName,PrimaryKey,read) of
      [] ->
        mnesia:write(TableName,Record,LockKind);
      _->
        <<"Duplicate already exist">>
    end
       end,
  mnesia:transaction(Fun).

%%======================================================================================================================
%% @doc - check_n_write_index
%% Handles all safe create - (checks db before writing) operations to the database using index key.
%%======================================================================================================================

check_n_write_index(TableName,Record,LockKind,PrimaryKey,Pos)->
  Fun= fun()->
    case mnesia:index_read(TableName,PrimaryKey,Pos) of
      [] ->
        mnesia:write(TableName,Record,LockKind);
      _->
        <<"Duplicate already exist">>
    end
       end,
  mnesia:transaction(Fun).

%%======================================================================================================================
%% @doc - multiple_write
%% Handles multiple create operation to the database (dirty operation).
%%======================================================================================================================

multiple_write(TableName,RecList) ->
  [mnesia:dirty_write(TableName,Rec) || Rec <- RecList],
  ok.

%%======================================================================================================================
%% @doc - multiple_read
%% Handles multiple read operation to the database (dirty operation).
%%======================================================================================================================

multiple_read(TableName,Keys) ->
  multiple_read(TableName,Keys,[]).

multiple_read(_TableName,[],Acc) -> Acc;
multiple_read(TableName,[Key|Rest],Acc) ->
  [UserRec] = mnesia:dirty_read(TableName,Key),
  multiple_read(TableName,Rest,[UserRec|Acc]).

%%======================================================================================================================
%% @doc - update
%% Handles updating an existing key/Value pair to the database using primary key.
%%======================================================================================================================

update(TableName,Key,Record)->
  Fun =fun()->
    [_R] = mnesia:wread({TableName,Key}),
           mnesia:write(Record)
       end,
  mnesia:transaction(Fun).

%%======================================================================================================================
%% @doc - search
%% Handles read operation to the database using primary key.
%%======================================================================================================================

search(TableName,Key)->
  Fun = fun()->
    Rec = mnesia:read(TableName,Key,read),
    case Rec of
      []->
        {Key,not_found};
      _->
        Rec
    end
        end,
  mnesia:transaction(Fun).

%%======================================================================================================================
%% @doc - index_search
%% Handles read operation to the database using index key.
%%======================================================================================================================

index_search(TableName,Key,Pos)->
  Fun = fun()-> mnesia:index_read(TableName,Key,Pos) end,
  mnesia:transaction(Fun).

%%======================================================================================================================
%% @doc - delete
%% Handles delete operation to the database using primary key.
%%======================================================================================================================


delete(TableName,Key)->
  Fun = fun()->
    case mnesia:read(TableName,Key,read) of
      [] ->
        {Key,not_existing};
      _->
        mnesia:delete(TableName,Key,write)
    end
        end,
  mnesia:transaction(Fun).

%%======================================================================================================================
%% @doc - delete
%% Handles delete operation to the database using primary key and return the last state of the key.
%%======================================================================================================================

delete_n_return(TableName,Key)->
  Fun = fun()->
    case mnesia:read(TableName,Key,read) of
      [] ->
        {Key,not_existing};
      Rec ->
        mnesia:delete(TableName,Key,write),
        Rec
    end
        end,
  mnesia:transaction(Fun).

%%======================================================================================================================
%% @doc - delete
%% Returns all the keys of the given table.
%%======================================================================================================================

all_keys(TableName) ->
  Fun = fun()-> mnesia:all_keys(TableName) end,
  mnesia:transaction(Fun).

%%======================================================================================================================
%% @doc - delete
%% Handles replacing the primary key (basically delete/write) of the entire data in the database.
%%======================================================================================================================

replace_key(TableName,OldKey,LockKind,Record) ->
  Fun = fun()->
    case mnesia:read(TableName,OldKey,read) of
      [] ->
        {error,not_existing};
      _->
        mnesia:delete(TableName,OldKey,LockKind),
        mnesia:write(TableName,Record,LockKind)
    end
        end,
  mnesia:transaction(Fun).

%%======================================================================================================================
%% @doc - delete
%% Handles creating a Key-Value pair in a table only if certain data is present in another table.
%%======================================================================================================================

check_n_write_multiple_tab(TableName1,TableName2,Key,LockKind,Record) ->
  Fun = fun()->
    case mnesia:read(TableName1,Key,read) of
      [] ->
        {error,not_existing};
      _  -> case mnesia:read(TableName2,Key,read) of
              [] ->
                mnesia:write(TableName2,Record,LockKind);
              _  ->
                <<"Duplicate already exist">>
            end
    end
        end,
  mnesia:transaction(Fun).
