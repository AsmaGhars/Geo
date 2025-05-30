import { Component } from '@angular/core';
import { Router } from '@angular/router';
import { HttpClient } from '@angular/common/http';

@Component({
  selector: 'app-choice',
  templateUrl: './choice.component.html',
  styleUrls: ['./choice.component.css']
})
export class ChoiceComponent {
  constructor(private router: Router) {}

  selectChoice(choice: string) {
    localStorage.setItem('projectChoice', choice)
    this.router.navigate(['/responsible']);
  }
}


