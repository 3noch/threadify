{-# LANGUAGE OverloadedStrings #-}

import           Control.Monad (mapM)
import           Data.Maybe (fromJust)
import           Data.List (foldl')
import           Data.String (fromString)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as Tio
import           Options.Applicative
import           Database.SQLite.Simple


data CmdOptions = CmdOptions
  { _file  :: String
  , _table :: String
  , _delim :: String }


cmdOptions :: Parser CmdOptions
cmdOptions = CmdOptions
  <$> strOption
      ( long    "file"
     <> short   'f'
     <> metavar "FILE"
     <> help    "SQLite database file to use for data" )
  <*> strOption
      ( long    "table"
     <> short   't'
     <> metavar "TABLE"
     <> value   "log_data"
     <> showDefault
     <> help    "Name of database table in FILE")
  <*> strOption
      ( long    "delimiter"
     <> short   'd'
     <> metavar "DELIM"
     <> value   "\t"
     <> help    "Delimiter to use for output (default: tab)")


main' :: CmdOptions -> IO ()
main' (CmdOptions file table delim') = withConnection file run
  where
    run c = do
      rowCnt    <- getRowCount table c
      threadIds <- getThreadIds table c
      threads   <- getThread table c `mapM` threadIds
      let ids = [[Just (x, ("", ""))] | x <- [0..rowCnt - 1]]
      let rows = foldl' (joinSparseThreads Nothing) ids threads
      Tio.putStrLn $ renderHeader delim threadIds
      Tio.putStrLn $ renderCsv    delim rows

    delim = T.pack delim'


main :: IO ()
main = execParser opts >>= main'
  where
    opts = info (helper <*> cmdOptions)
      ( fullDesc
     <> progDesc "Indent the last column using the penultimate column as a scope name" )


(<$$>) = fmap . fmap


getThreadIds :: String -> Connection -> IO [Text]
getThreadIds table c = fromOnly <$$> query_ c q
  where
    q = fromString $ "SELECT thread_id FROM ("
     <>   "SELECT thread_id, COUNT(*) AS cnt FROM " <> table
     <>   " GROUP BY thread_id ORDER BY cnt DESC)"


getRowCount :: String -> Connection -> IO Int
getRowCount table c = head <$> fromOnly <$$> query_ c (fromString $ "SELECT COUNT(*) FROM " <> table)


type ThreadEntry = Maybe (Int, (Text, Text))

getThread :: String -> Connection -> Text -> IO [ThreadEntry]
getThread table c threadId = rekey <$$> query c q [threadId]
  where
    q = fromString $ "SELECT id, func, message FROM " <> table <> " WHERE thread_id = ? ORDER BY id"
    rekey (x, y, z) = Just (x, (y, z))


joinSparseColumns :: ([a] -> a -> Bool) -> a -> [[a]] -> [a] -> [[a]]
joinSparseColumns _ _   []     _  = []
joinSparseColumns p def (a:as) [] = (a ++ [def]) : joinSparseColumns p def as []
joinSparseColumns p def (a:as) (b:bs)
  | a `p` b   = (a ++ [b])   : joinSparseColumns p def as bs
  | otherwise = (a ++ [def]) : joinSparseColumns p def as (b:bs)


joinSparseThreads :: ThreadEntry -> [[ThreadEntry]] -> [ThreadEntry] -> [[ThreadEntry]]
joinSparseThreads = joinSparseColumns p
  where
    p :: [ThreadEntry] -> ThreadEntry -> Bool
    p (Just (a,_):_) (Just (b,_)) = a == b


renderIdx :: ThreadEntry -> Text
renderIdx (Just (idx, _)) = T.pack (show idx)

renderCol :: ThreadEntry -> Text
renderCol Nothing = ""
renderCol (Just (_, (func, msg))) = func <> " - " <> msg


renderCsv :: Text -> [[ThreadEntry]] -> Text
renderCsv delim rows = T.unlines $ renderRow <$> rows
  where
    renderRow :: [ThreadEntry] -> Text
    renderRow (c:cs) = T.intercalate delim $ renderIdx c : (renderCol <$> cs)

renderHeader :: Text -> [Text] -> Text
renderHeader delim thIds = T.intercalate delim $ "Line" : thIds

