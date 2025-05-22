BEGIN
  DECLARE txt STRING DEFAULT 'Hello '
  SET txt = txt || 'Lakehouse';
  IF substr(txt, -1, 1) NOT IN ('.', '!', '?') THEN
    SET txt = txt || '!';
  END IF;
  SELECT txt;
END;
