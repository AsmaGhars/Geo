import { Component } from '@angular/core';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  title = 'frontend';
  choiceSelected: boolean = false;
  inputsValidated: boolean = false;
  email: string | null = null;
  message: string | null = null;

  onBackToChoice() {
    this.choiceSelected = false;
    this.inputsValidated = false;
    this.email = null;
    this.message = null;
  }
}
