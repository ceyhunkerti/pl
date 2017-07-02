declare

begin

  util.pl.logger := util.logtype.init('log.test');
  util.pl.logger.info('info with init');
  util.pl.logger.success('success with init');
  util.pl.logger.error('error with init');

  util.pl.logger.info('test.name', 'info w/o init');
  util.pl.logger.success('test.name', 'success w/o init');
  util.pl.logger.error('test.name', 'error info w/o init');


end;