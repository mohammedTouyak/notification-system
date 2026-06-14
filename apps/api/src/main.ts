import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.setGlobalPrefix('api');

  app.useGlobalPipes( // ValidationPipe pour valider les données entrantes et transformer les types.
    new ValidationPipe({
      whitelist: true, // Supprime les champs non déclarés dans le DTO.
      forbidNonWhitelisted: true, // Retourne une erreur si le body contient un champ interdit.
      transform: true, // Transforme le body en instance du DTO.
    }),
  );

  const port = process.env.PORT ?? 3000;
  await app.listen(port);

  console.log(`API is running on http://localhost:${port}/api`);
}

bootstrap();
